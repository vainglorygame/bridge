#!/usr/bin/env node
/* jshint esnext: true */
"use strict";

const amqp = require("amqplib"),
    winston = require("winston"),
    Seq = require("sequelize"),
    request = require("request-promise"),
    express = require("express"),
    http = require("http"),
    sleep = require("sleep-promise");

const MADGLORY_TOKEN = process.env.MADGLORY_TOKEN,
    DATABASE_URI = process.env.DATABASE_URI,
    RABBITMQ_URI = process.env.RABBITMQ_URI || "amqp://localhost",
    REGIONS = ["na", "eu", "sg", "sa", "ea"];
if (MADGLORY_TOKEN == undefined) throw "Need an API token";

const app = express(),
    server = http.Server(app),
    logger = new (winston.Logger)({
        transports: [
            new (winston.transports.Console)({
                timestamp: () => Date.now(),
                formatter: (options) => winston.config.colorize(options.level,
`${new Date(options.timestamp()).toISOString()} ${options.level.toUpperCase()} ${(options.message? options.message:"")} ${(options.meta && Object.keys(options.meta).length? JSON.stringify(options.meta):"")}`)
            })
        ]
    });
let rabbit, ch, seq, model;

// connect to broker, retrying forever
(async () => {
    while (true) {
        try {
            seq = new Seq(DATABASE_URI, { logging: () => {} });
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
})();

server.listen(8880);
app.use(express.static("assets"));

function grabPlayer(name, region, last_match_created_date, id) {
    last_match_created_date = last_match_created_date || new Date(0);

    // add 1s, because createdAt-start <= x <= createdAt-end
    // so without the +1s, we'd always get the last_match_created_date match back
    last_match_created_date.setSeconds(last_match_created_date.getSeconds() + 1);

    const payload = {
        "region": region,
        "params": {
            "filter[playerIds]": id,
            "filter[createdAt-start]": last_match_created_date.toISOString(),
            "filter[gameMode]": "casual,ranked",
            "sort": "-createdAt"
        }
    };
    logger.info("requesting update for '%s' in '%s'", name, region);

    return ch.sendToQueue("grab", new Buffer(JSON.stringify(payload)), {
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
        logger.info("searching '%s' ('%s') in '%s'", name, id, region);
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

            logger.info("found '%s' ('%s') in '%s'", name, id, region);
            found = true;
            break;
        } catch (err) {
            response = err.response;
            if (err.statusCode == 429) {
                logger.warn("rate limited, sleeping");
                await sleep(100);  // no return, no break => retry
            } else if (err.statusCode != 404) console.error(err);
            if (err.statusCode != 429) {
                logger.warn("did not find '%s' ('%s') in '%s')", name, id, region,
                    {uri: err.options.uri, qs: err.options.qs, error: err.response.body});
                return [];
            }
        } finally {
            logger.info("API response: status %s, connection start %s, connection end %s, ratelimit remaining: %s",
                response.statusCode, response.timings.connect, response.timings.end, response.headers["x-ratelimit-remaining"]);
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
    logger.info("searching '%s'", name);
    await Promise.all(REGIONS.map(async (region) => {
        let players = await searchPlayerInRegion(region, name, undefined);
        if (players.length > 0)
            found = true;

        // request grab jobs
        await Promise.all(players.map((p) =>
            grabPlayer(p.attributes.name, p.attributes.shardId, undefined, p.id)));
    }));
    // notify web
    if (!found)
        await ch.publish("amq.topic", "player." + name,
            new Buffer("search_fail"));
}

// update a player from API based on db record
// & grab player
async function updatePlayer(player) {
    // set last_update and request an update job
    // if last_update is null, we need that player's full history
    let grabstart;
    if (player.get("last_update") == null) grabstart = undefined;
    else {
        let last_match = await model.Participant.findOne({
            where: { player_api_id: player.get("api_id") },
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
    await Promise.all(players.map((p) =>
        grabPlayer(p.attributes.name, p.attributes.shardId, grabstart, p.id)));
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
        last_update = (new Date(value=0)).toISOString();
    else last_update = record.get("value");

    logger.info("updating samples in '%s' since %s", region, last_update);
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
        logger.warn("player '%s' not found in db, searching instead", req.params.name);
        searchPlayer(req.params.name);  // fire away
        res.sendStatus(204);
        return;
    }
    logger.info("player '%s' in db, updating", req.params.name);
    updatePlayer(player);  // fire away
    res.sendStatus(204);
});
// crunch a known user
app.post("/api/player/:name/crunch", async (req, res) => {
    let player = await model.Player.findOne({ where: { name: req.params.name } });
    if (player == undefined) {
        logger.warn("player '%s' not found in db, won't crunch", req.params.name);
        res.sendStatus(404);
        return;
    }
    logger.info("player '%s' in db, crunching", req.params.name);
    await ch.sendToQueue("crunch", new Buffer(player.api_id),
        { persistent: true, type: "player" });
    res.sendStatus(204);
});
// update a random user
app.post("/api/player", async (req, res) => {
    let player = await model.Player.findOne({ order: [ Seq.fn("RAND") ] });
    updatePlayer(player);  // do not await
    res.json(player);
});
// download sample sets
app.post("/api/samples/:region", async (req, res) => {
    await updateSamples(req.params.region);
    res.sendStatus(204);
});
app.post("/api/samples", async (req, res) => {
    await Promise.all(REGIONS.map((region) =>
        updateSamples(region)));
    res.sendStatus(204);
});
// crunch global stats
app.post("/api/crunch", async (req, res) => {
    logger.info("crunching global stats");
    await ch.sendToQueue("crunch", new Buffer(""),
        { persistent: true, type: "global" });
    res.sendStatus(204);
});

/* internal monitoring */
app.get("/", (req, res) => {
    res.sendFile(__dirname + "/index.html");
});
