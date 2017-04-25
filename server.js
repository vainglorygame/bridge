#!/usr/bin/env node
/* jshint esnext: true */
"use strict";
const amqp = require("amqplib"),
    Promise = require("bluebird"),
    winston = require("winston"),
    loggly = require("winston-loggly-bulk"),
    Seq = require("sequelize"),
    request = require("request-promise"),
    express = require("express"),
    http = require("http"),
    sleep = require("sleep-promise");

const MADGLORY_TOKEN = process.env.MADGLORY_TOKEN,
    DATABASE_URI = process.env.DATABASE_URI,
    DATABASE_BRAWL_URI = process.env.DATABASE_BRAWL_URI,
    RABBITMQ_URI = process.env.RABBITMQ_URI || "amqp://localhost",
    LOGGLY_TOKEN = process.env.LOGGLY_TOKEN,
    BRAWL = process.env.BRAWL != "false",
    REGIONS = ["na", "eu", "sg", "sa", "ea"];
if (MADGLORY_TOKEN == undefined) throw "Need an API token";

const app = express(),
    server = http.Server(app),
    logger = new (winston.Logger)({
        transports: [
            new (winston.transports.Console)({
                timestamp: true,
                colorize: true
            })
        ]
    });
let rabbit, ch, seq, seqBrawl, model, modelBrawl;

// loggly integration
if (LOGGLY_TOKEN)
    logger.add(winston.transports.Loggly, {
        inputToken: LOGGLY_TOKEN,
        subdomain: "kvahuja",
        tags: ["backend", "bridge"],
        json: true
    });

// connect to broker, retrying forever
(async () => {
    while (true) {
        try {
            seq = new Seq(DATABASE_URI, { logging: () => {} });
            if (BRAWL)
                seqBrawl = new Seq(DATABASE_BRAWL_URI, { logging: () => {} });
            rabbit = await amqp.connect(RABBITMQ_URI);
            ch = await rabbit.createChannel();
            await ch.assertQueue("grab", {durable: true});
            await ch.assertQueue("process", {durable: true});
            await ch.assertQueue("crunch", {durable: true});
            break;
        } catch (err) {
            logger.error("Error connecting", err);
            await sleep(5000);
        }
    }
    model = require("../orm/model")(seq, Seq);
    if (BRAWL)
        modelBrawl = require("../orm/model")(seqBrawl, Seq);
})();

server.listen(8880);
app.use(express.static("assets"));

function grabPlayer(name, region, last_match_created_date, id, gameModes) {
    last_match_created_date = last_match_created_date || new Date(0);

    // add 1s, because createdAt-start <= x <= createdAt-end
    // so without the +1s, we'd always get the last_match_created_date match back
    last_match_created_date.setSeconds(last_match_created_date.getSeconds() + 1);

    const modes = (gameModes == "brawl") ? "blitz_pvp_ranked,casual_aral" : "casual,ranked",
        payload = {
        "region": region,
        "params": {
            "filter[playerIds]": id,
            "filter[createdAt-start]": last_match_created_date.toISOString(),
            "filter[gameMode]": modes,
            "sort": "-createdAt"
        }
    };
    logger.info("requesting update", { name: name, region: region });

    // TODO make this more dynamic
    const queue = (gameModes == "brawl") ? "grab_brawl" : "grab";
    return ch.sendToQueue(queue, new Buffer(JSON.stringify(payload)), {
        persistent: true,
        type: "matches",
        headers: { notify: "player." + name }
    });
}

// search for a player name in one region
// request process for lifetime
// return an array (JSONAPI response)
async function searchPlayerInRegion(region, name, id) {
    let response,
        players = [],
        found = false;
    while (true) {
        logger.info("searching", { name: name, id: id, region: region });
        try {
            // find players by name
            let opts = {
                uri: "https://api.dc01.gamelockerapp.com/shards/" + region + "/players",
                headers: {
                    "X-Title-Id": "semc-vainglory",
                    "Authorization": MADGLORY_TOKEN
                },
                qs: {},
                json: true,
                gzip: true,
                time: true,
                forever: true,
                strictSSL: true,
                resolveWithFullResponse: true
            };
            // prefer player id over name
            if (id == undefined) opts["qs"]["filter[playerNames]"] = name
            else opts["qs"]["filter[playerIds]"] = id
            logger.info("API request", {uri: opts.uri, qs: opts.qs});
            response = await request(opts);
            players = response.body;

            logger.info("found", { name: name, id: id, region: region });
            found = true;
            break;
        } catch (err) {
            response = err.response;
            if (err.statusCode == 429) {
                logger.warn("rate limited, sleeping");
                await sleep(100);  // no return, no break => retry
            } else if (err.statusCode != 404) logger.error(err);
            if (err.statusCode != 429) {
                logger.warn("not found", name, id, region,
                    { name: name, id: id, region: region, uri: err.options.uri, qs: err.options.qs, error: err.response.body });
                return [];
            }
        } finally {
            logger.info("API response",
                { status: response.statusCode, connection_start: response.timings.connect, connection_end: response.timings.end, ratelimit_remaining: response.headers["x-ratelimit-remaining"] });
        }
    }

    if (!found) return [];

    // notify web that data is being loaded
    await ch.publish("amq.topic", "player." + name,
        new Buffer("search_success"));
    // players.length will be 1 in 99.9% of all cases
    // - but this will cover the 0.01% too
    //
    // send to processor, so the player is in db
    // no matter whether we find matches or not
    await ch.sendToQueue("process", new Buffer(JSON.stringify(players.data)),
        { persistent: true, type: "player" });

    return players.data;
}

// search for player name on all shards
// grab player(s)
// send a notification for results and request updates
async function searchPlayer(name) {
    let found = false;
    logger.info("searching", { name: name });
    await Promise.map(REGIONS, async (region) => {
        let players = await searchPlayerInRegion(region, name, undefined);
        if (players.length > 0)
            found = true;

        // request grab jobs
        await Promise.map(players, (p) =>
            grabPlayer(p.attributes.name, p.attributes.shardId,
                undefined, p.id, "regular"));
    });
    // notify web
    if (!found)
        await ch.publish("amq.topic", "player." + name,
            new Buffer("search_fail"));
}

// update a player from API based on db record
// & grab player
async function updatePlayer(player, gameModes) {
    // set last_update and request an update job
    // if last_update is null, we need that player's full history
    let grabstart;
    if (player.get("last_update") == null) grabstart = undefined;
    else {
        const m = (gameModes == "brawl")? modelBrawl : model,
            last_match = await m.Participant.findOne({
            where: { player_api_id: player.get("api_id"), },
            attributes: ["created_at"],
            order: [ [seq.col("created_at"), "DESC"] ]
        });
        if (last_match == null) grabstart = undefined;
        else grabstart = last_match.get("created_at");
    }

    const players = await searchPlayerInRegion(
        player.get("shard_id"), player.get("name"), player.get("api_id"));

    // will be the same as `player` 99.9% of the time
    // but not when the player changed their name
    // search happened by ID, so we get only 1 player back
    // but player.name != players[0].attributes.name
    // (with a name change)
    await Promise.map(players, (p) =>
        grabPlayer(p.attributes.name, p.attributes.shardId,
            grabstart, p.id, gameModes));
}

// update a region's samples since samples_last_update
async function updateSamples(region) {
    let record = await model.Keys.findOne({
        where: {
            type: "samples_last_update",
            key: region
        }
    }), last_update;
    if (record == undefined)
        last_update = (new Date(0)).toISOString();
    else last_update = record.get("value");

    logger.info("updating samples", { region: region, last_update: last_update });
    await ch.sendToQueue("grab", new Buffer(
        JSON.stringify({ region: region,
            params: {
                sort: "-createdAt",
                "filter[createdAt-start]": last_update
            }
        })),
        { persistent: true, type: "samples" });

    last_update = (new Date()).toISOString();
    if (record == null)
        await model.Keys.create({
            type: "samples_last_update",
            key: region,
            value: last_update
        });
    else
        await record.update({ value: last_update });
}

// wipe all points that meet the `where` condition and recrunch
async function crunchPlayer(api_id) {
    logger.info("deleting all player points", { api_id: api_id });
    await model.PlayerPoint.destroy({ where: { player_api_id: api_id } });
    // get all participants for this player
    const participations = await model.Participant.findAll({
        attributes: ["api_id"],
        where: { player_api_id: api_id } });
    // send everything to cruncher
    logger.info("sending participations to cruncher",
        { length: participations.length });
    await Promise.map(participations, async (p) =>
        await ch.sendToQueue("crunch", new Buffer(p.api_id),
            { persistent: true, type: "player" }));
    // jobs with the type "player" won't be taken into account for global stats
    // global stats would increase on every player refresh otherwise
}

// crunch (force = recrunch) global stats
async function crunchGlobal(force=false) {
    // get lcpid from keys table
    let last_crunch_participant_id = (await model.Keys.findOrCreate({
        where: {
            type: "crunch",
            key: "global_last_crunch_participant_id"
        },
        defaults: {
            value: 0
        }
    }))[0];

    if (force) {
        // refresh everything
        logger.info("deleting all global points");
        await model.GlobalPoint.destroy({ truncate: true });
        await last_crunch_participant_id.update({ value: 0 });
    }

    // don't load the whole Participant table at once into memory
    const batchsize = 1000;
    let offset = 0, participations;

    logger.info("loading all participations into cruncher");
    do {
        logger.info("loading more participations into cruncher",
            { offset: offset, limit: batchsize });
        participations = await model.Participant.findAll({
            attributes: ["api_id", "id"],
            limit: batchsize,
            offset: offset,
            order: [ ["id", "ASC"] ]
        });
        await Promise.map(participations, async (p) =>
            await ch.sendToQueue("crunch", new Buffer(p.api_id),
                { persistent: true, type: "global" }));
        offset += batchsize;
        if (participations.length > 0)
            await last_crunch_participant_id.update({
                value: participations[participations.length-1].id
            });
    } while (participations.length == batchsize);
    logger.info("done loading participations into cruncher");
}

/* routes */
// force an update
app.post("/api/player/:name/search", async (req, res) => {
    searchPlayer(req.params.name);  // do not await, just fire
    res.sendStatus(204);  // notifications will follow
});
// update a known user
// flow:
//   * get from db
//   * update lifetime from `/players` via id, catch IGN changes
//   * update from `/matches`
app.post("/api/player/:name/update", async (req, res) => {
    let player = await model.Player.findOne({ where: { name: req.params.name } });
    if (player == undefined) {
        logger.warn("player not found in db, searching instead", { name: req.params.name });
        searchPlayer(req.params.name);  // fire away
        res.sendStatus(204);
        return;
    }
    logger.info("player in db, updating", { name: req.params.name });
    updatePlayer(player, "regular");  // fire away
    res.sendStatus(204);
});
// get brawls for a known user
app.post("/api/player/:name/update-brawl", async (req, res) => {
    let player = await model.Player.findOne({ where: { name: req.params.name } });
    if (player == undefined) {
        logger.error("player not found in db", { name: req.params.name });
        res.sendStatus(404);
        return;
    }
    logger.info("player in db, updating brawls", { name: req.params.name });
    updatePlayer(player, "brawl");  // fire away
    res.sendStatus(204);
});
// crunch a known user
app.post("/api/player/:name/crunch", async (req, res) => {
    const player = await model.Player.findOne({ where: { name: req.params.name } });
    if (player == undefined) {
        logger.warn("player not found in db, won't recrunch",
            { name: req.params.name });
        res.sendStatus(404);
        return;
    }
    logger.info("player in db, recrunching", { name: req.params.name });
    crunchPlayer(player.api_id);  // fire away
    res.sendStatus(204);
});
// update a random user
app.post("/api/player", async (req, res) => {
    let player = await model.Player.findOne({ order: [ Seq.fn("RAND") ] });
    updatePlayer(player, "regular");  // do not await
    res.json(player);
});
// download sample sets
app.post("/api/samples/:region", async (req, res) => {
    await updateSamples(req.params.region);
    res.sendStatus(204);
});
app.post("/api/samples", async (req, res) => {
    await Promise.map(REGIONS, (region) =>
        updateSamples(region));
    res.sendStatus(204);
});
// download Telemetry
app.post("/api/match/:match/telemetry", async (req, res) => {
    logger.info("requesting download for Telemetry", { api_id: req.params.api_id });
    await ch.sendToQueue("grab", new Buffer(req.params.api_id),
        { persistent: true, type: "telemetry" });
    res.sendStatus(204);
});
// crunch all global stats
app.post("/api/crunch", async (req, res) => {
    crunchGlobal(false);  // fire away
    res.sendStatus(204);
});
// force updating all stats
app.post("/api/recrunch", async (req, res) => {
    crunchGlobal(true);  // fire away
    res.sendStatus(204);
});

/* internal monitoring */
app.get("/", (req, res) => {
    res.sendFile(__dirname + "/index.html");
});

process.on("unhandledRejection", function(reason, promise) {
    logger.error(reason);
});
