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
    sleep = require("sleep-promise"),
    api = require("../orm/api");

const DATABASE_URI = process.env.DATABASE_URI,
    DATABASE_BRAWL_URI = process.env.DATABASE_BRAWL_URI,
    DATABASE_TOURNAMENT_URI = process.env.DATABASE_TOURNAMENT_URI,
    RABBITMQ_URI = process.env.RABBITMQ_URI || "amqp://localhost",
    LOGGLY_TOKEN = process.env.LOGGLY_TOKEN,
    BRAWL = process.env.BRAWL != "false",
    TOURNAMENT = process.env.TOURNAMENT != "false",
    REGIONS = (process.env.REGIONS || "na,eu,sg,sa,ea").split(","),
    TOURNAMENT_REGIONS = (process.env.TOURNAMENT_REGIONS || "tournament-na," +
        "tournament-eu,tournament-sg,tournament-sa,tournament-ea").split(","),
    GRABSTART = process.env.GRABSTART || "2017-01-01T00:00:00Z",
    BRAWL_RETENTION_DAYS = parseInt(process.env.BRAWL_RETENTION_DAYS) || 3,
    PLAYER_PROCESS_QUEUE = process.env.PLAYER_PROCESS_QUEUE || "process",
    GRAB_QUEUE = process.env.GRAB_QUEUE || "grab",
    GRAB_BRAWL_QUEUE = process.env.GRAB_BRAWL_QUEUE || "grab_brawl",
    GRAB_TOURNAMENT_QUEUE = process.env.GRAB_TOURNAMENT_QUEUE || "grab_tournament",
    CRUNCH_QUEUE = process.env.CRUNCH_QUEUE || "crunch",
    CRUNCH_TOURNAMENT_QUEUE = process.env.CRUNCH_TOURNAMENT_QUEUE || "crunch_tournament",
    ANALYZE_QUEUE = process.env.ANALYZE_QUEUE || "analyze",
    SAMPLE_QUEUE = process.env.SAMPLE_QUEUE || "sample",
    SHOVEL_SIZE = parseInt(process.env.SHOVEL_SIZE) || 1000;

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
let rabbit, ch,
    seq, seqBrawl, seqTournament,
    model, modelBrawl, modelTournament;

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
            if (TOURNAMENT)
                seqTournament = new Seq(DATABASE_TOURNAMENT_URI, { logging: () => {} });
            rabbit = await amqp.connect(RABBITMQ_URI);
            ch = await rabbit.createChannel();
            await ch.assertQueue(GRAB_QUEUE, {durable: true});
            await ch.assertQueue(GRAB_BRAWL_QUEUE, {durable: true});
            await ch.assertQueue(GRAB_TOURNAMENT_QUEUE, {durable: true});
            await ch.assertQueue(PLAYER_PROCESS_QUEUE, {durable: true});
            await ch.assertQueue(CRUNCH_QUEUE, {durable: true});
            await ch.assertQueue(CRUNCH_TOURNAMENT_QUEUE, {durable: true});
            await ch.assertQueue(SAMPLE_QUEUE, {durable: true});
            break;
        } catch (err) {
            logger.error("Error connecting", err);
            await sleep(5000);
        }
    }
    model = require("../orm/model")(seq, Seq);
    if (BRAWL)
        modelBrawl = require("../orm/model")(seqBrawl, Seq);
    if (TOURNAMENT)
        modelTournament = require("../orm/model")(seqTournament, Seq);
})();

server.listen(8880);
app.use(express.static("assets"));

// convenience mapping
// `category`: "regular", "brawl", "tournament"
// TODO refactor this
function gameModesForCategory(category) {
    switch (category) {
        case "regular": return "casual,ranked";
        case "brawl": return "casual_aral,blitz_pvp_ranked";
        case "tournament": return "private,private_party_draft_match";
        default: logger.error("unsupported game mode category",
            { category: category });
    }
}

// convenience function to sort into the right grab queue
function grabQueueForCategory(category) {
    switch (category) {
        case "regular": return GRAB_QUEUE;
        case "brawl": return GRAB_BRAWL_QUEUE;
        case "tournament": return GRAB_TOURNAMENT_QUEUE;
        default: logger.error("unsupported game mode category",
            { category: category });
    }
}

function crunchQueueForCategory(category) {
    switch (category) {
        case "regular": return CRUNCH_QUEUE;
        case "tournament": return CRUNCH_TOURNAMENT_QUEUE;
        default: logger.error("unsupported game mode category",
            { category: category });
    }
}

// request grab jobs for a player's matches
async function grabPlayer(name, region, last_match_created_date, id, category) {
    const payload = {
        "region": region,
        "params": {
            "filter[playerIds]": id,
            "filter[createdAt-start]": last_match_created_date,  // ISO8601 string
            "filter[gameMode]": gameModesForCategory(category),
            "sort": "createdAt"
        }
    };
    logger.info("requesting update", { name: name, region: region });

    await ch.sendToQueue(grabQueueForCategory(category),
            new Buffer(JSON.stringify(payload)), {
        persistent: true,
        type: "matches",
        headers: { notify: "player." + name }
    });
}
// request grab job for multiple players' matches
async function grabPlayers(names, region, grabstart, category) {
    const payload = {
        "region": region,
        "params": {
            "filter[playerNames]": names,
            "filter[createdAt-start]": grabstart.toISOString(),
            "filter[gameMode]": gameModesForCategory(category),
            "sort": "createdAt"
        }
    };
    logger.info("requesting triple player grab", {
        names: names,
        region: region,
        category: category
    });

    await ch.sendToQueue(grabQueueForCategory(category),
            new Buffer(JSON.stringify(payload)), {
        persistent: true,
        type: "matches",
        headers: { notify: "player." + names }
    });
}

// request grab jobs for a region's matches
async function grabMatches(region, last_match_created_date) {
    last_match_created_date = last_match_created_date || new Date(0);

    // add 1s, because createdAt-start <= x <= createdAt-end
    // so without the +1s, we'd always get the last_match_created_date match back
    last_match_created_date.setSeconds(last_match_created_date.getSeconds() + 1);

    const payload = {
        "region": region,
        "params": {
            "filter[createdAt-start]": last_match_created_date.toISOString(),
            "sort": "createdAt",
            "page[limit]": 5  // TODO maximum for `/matches`
        }
    };
    logger.info("requesting region update", { region: region });

    await ch.sendToQueue(GRAB_TOURNAMENT_QUEUE, new Buffer(JSON.stringify(payload)), {
        persistent: true,
        type: "matches"
    });
}

// search for a player name in one region
// request process for lifetime
// return an array (JSONAPI response)
async function searchPlayerInRegion(region, name, id) {
    logger.info("searching", { name: name, id: id, region: region });
    let options = {};
    if (id == undefined) options["filter[playerNames]"] = name
    else options["filter[playerIds]"] = id

    // players.length and page length will be 1 in 99.9999% of all cases
    // - but just in case.
    const players = await Promise.map(await api.requests("players",
        region, options, logger),
        async (player) => {
            logger.info("found", { name: name, id: id, region: region });
            // notify web that data is being loaded
            await ch.publish("amq.topic", "player." + name,
                new Buffer("search_success"));
            // send to processor, so the player is in db
            // no matter whether we find matches or not
            await ch.sendToQueue(PLAYER_PROCESS_QUEUE,
                new Buffer(JSON.stringify(player)), {
                    persistent: true, type: "player",
                    headers: { notify: "player." + player.name }
                });
            return player;
        }
    );
    if (players.length == 0)
        logger.warn("not found", { name: name, id: id, region: region });

    return players;
}

// search for player name on all shards
// grab player(s)
// send a notification for results and request updates
async function searchPlayer(name) {
    let found = false;
    logger.info("searching", { name: name });
    await Promise.map(REGIONS, async (region) => {
        const players = await searchPlayerInRegion(region, name, undefined);
        if (players.length > 0) found = true;

        // request grab jobs
        await Promise.map(players, (p) =>
            grabPlayer(p.name, p.shardId,
                defaultGrabstartForCategory("regular"), p.id, "regular"));
    });
    // notify web
    if (!found) {
        logger.info("search failed", { name: name });
        await ch.publish("amq.topic", "player." + name,
            new Buffer("search_fail"));
    }
}

// return the fitting db connection based on game mode
// `category` is an enum (brawl, regular, tournament)
function databaseForCategory(category) {
    if (category == "brawl")
        return modelBrawl;
    if (category == "tournament")
        return modelTournament;
    if (category == "regular")
        return model;
    logger.error("unsupported category", { category: category });
}

// return a default `createdAt-start` based on game mode
function defaultGrabstartForCategory(category) {
    if (category == "brawl") {
        let past = new Date();
        past.setDate(past.getDate() - BRAWL_RETENTION_DAYS);
        return past.toISOString();  // minimum
    }
    if (category == "tournament")
        return "2017-01-01T00:00:00Z";  // all
    if (category == "regular")
        return GRABSTART;  // as via env var
}

// update a player from API based on db record
// & grab player
async function updatePlayer(player, category) {
    // set last_update and request an update job
    // if last_update is null, we need that player's full history
    let grabstart;
    if (player.get("last_update") != null)
        grabstart = player.get("created_at");  // TODO add 1s here!!!
    if (grabstart == undefined) grabstart = defaultGrabstartForCategory(category);
    else
        // add 1s, because createdAt-start <= x <= createdAt-end
        // so without the +1s, we'd always get the last_match_created_date match back
        grabstart.setSeconds(grabstart.getSeconds() + 1);

    const players = await searchPlayerInRegion(
        player.get("shard_id"), player.get("name"), player.get("api_id"));

    // will be the same as `player` 99.9% of the time
    // but not when the player changed their name
    // search happened by ID, so we get only 1 player back
    // but player.name != players[0].attributes.name
    // (with a name change)
    await Promise.map(players, async (p) =>
        await grabPlayer(p.name, p.shardId,
            grabstart, p.id, category));
}

// update a region from API based on db records
async function updateRegion(region, category) {
    let grabstart;
    const last_match = await databaseForCategory(category).Participant.findOne({
        attributes: ["created_at"],
        where: { shard_id: region },
        order: [ [seq.col("created_at"), "DESC"] ]
    });
    if (last_match == null) grabstart = undefined;
    else grabstart = last_match.get("created_at");

    await grabMatches(region, grabstart);
}

// update a region's samples since samples_last_update
async function updateSamples(region) {
    const last_update = await getKey(model, "samples_last_update",
        region, (new Date(0)).toISOString());

    logger.info("updating samples", { region: region, last_update: last_update });
    await ch.sendToQueue(GRAB_QUEUE, new Buffer(
        JSON.stringify({ region: region,
            params: {
                sort: "createdAt",
                "filter[createdAt-start]": last_update
            }
        })),
        { persistent: true, type: "samples" });

    last_update = (new Date()).toISOString();
    await setKey(model, "samples_last_update", region, last_update);
}

// upcrunch player's stats
async function crunchPlayer(category, api_id) {
    const db = databaseForCategory(category),
        last_crunch = await db.PlayerPoint.findOne({
            attributes: ["updated_at"],
            where: { player_api_id: api_id },
            order: [ ["updated_at", "DESC"] ]
        }),
        last_crunch_date = last_crunch == undefined?
            new Date(0) : last_crunch.updated_at;
    // get all participants for this player
    const participations = await db.Participant.findAll({
        attributes: ["api_id"],
        where: {
            player_api_id: api_id,
            created_at: {
                $gt: last_crunch_date
            }
        } });
    // send everything to cruncher
    logger.info("sending participations to cruncher",
        { length: participations.length });
    await Promise.map(participations, async (p) =>
        await ch.sendToQueue(crunchQueueForCategory(category),
            new Buffer(p.api_id),
            { persistent: true, type: "player" }));
    // jobs with the type "player" won't be taken into account for global stats
    // global stats would increase on every player refresh otherwise
}

// upanalyze player's matches (TrueSkill)
async function analyzePlayer(api_id) {
    const participations = await model.Participant.findAll({
        attributes: ["match_api_id"],
        where: {
            player_api_id: api_id,
            trueskill_mu: null  // where not analyzed yet
        },
        order: [ ["created_at", "ASC"] ]
    });
    logger.info("sending matches to analyzer",
        { length: participations.length });
    await Promise.map(participations, async (p) =>
        await ch.sendToQueue(ANALYZE_QUEUE, new Buffer(p.match_api_id),
            { persistent: true }));
}

// reset fame and crunch
// TODO: incremental crunch possible?
async function crunchTeam(team_id) {
    await ch.sendToQueue(CRUNCH_QUEUE, new Buffer(team_id),
        { persistent: true, type: "team" });
}

// crunch (force = recrunch) global stats
async function crunchGlobal(category, force=false) {
    const db = databaseForCategory(category);
    // get lcpid from keys table
    let last_crunch_participant_id = await getKey(db,
        "crunch", "global_last_crunch_participant_id", 0);

    if (force) {
        // refresh everything
        logger.info("deleting all global points");
        await db.GlobalPoint.destroy({ truncate: true });
        last_crunch_participant_id = await setKey(db, "crunch",
            "global_last_crunch_participant_id", 0);
    }

    // don't load the whole Participant table at once into memory
    let offset = 0, participations;

    logger.info("loading all participations into cruncher",
        { last_crunch_participant_id: last_crunch_participant_id });
    do {
        participations = await db.Participant.findAll({
            attributes: ["api_id", "id"],
            where: {
                id: { $gt: last_crunch_participant_id }
            },
            limit: SHOVEL_SIZE,
            offset: offset,
            order: [ ["id", "ASC"] ]
        });
        await Promise.map(participations, async (p) =>
            await ch.sendToQueue(crunchQueueForCategory(category),
                new Buffer(p.api_id),
                { persistent: true, type: "global" }));
        offset += SHOVEL_SIZE;
        if (participations.length > 0)
            await setKey(db, "crunch", "global_last_crunch_participant_id",
                participations[participations.length-1].id);
        logger.info("loading more participations into cruncher",
            { offset: offset, limit: SHOVEL_SIZE, size: participations.length });
    } while (participations.length == SHOVEL_SIZE);
    logger.info("done loading participations into cruncher");
}

// upanalyze all matches  TODO add force option
async function analyzeGlobal() {
    let offset = 0, matches;
    do {
        matches = await model.Match.findAll({
            attributes: ["api_id"],
            where: {
                trueskill_quality: null
            },
            limit: SHOVEL_SIZE,
            offset: offset,
            order: [ ["created_at", "ASC"] ]
        });
        await Promise.map(matches, async (m) =>
            await ch.sendToQueue(ANALYZE_QUEUE, new Buffer(m.api_id),
                { persistent: true }));
        offset += SHOVEL_SIZE;
        logger.info("loading more matches into analyzer",
            { offset: offset, limit: SHOVEL_SIZE, size: matches.length });
    } while (matches.length == SHOVEL_SIZE);
    logger.info("done loading matches into analyzer");
}

// return an entry from keys db
async function getKey(db, config, key, default_value) {
    const record = await db.Keys.findOrCreate({
        where: {
            type: config,
            key: key
        },
        defaults: { value: default_value }
    });
    return record[0].value;
}

// update an entry from keys db
async function setKey(db, config, key, value) {
    const record = await db.Keys.findOrCreate({
        where: {
            type: config,
            key: key
        },
        defaults: { value: value }
    });
    record[0].update({ value: value });
    return value;
}

/* routes */
// update a region, used for tournament shard
app.post("/api/match/:region/update", async (req, res) => {
    const region = req.params.region;
    if (TOURNAMENT_REGIONS.indexOf(region) == -1) {
        console.error("called with unsupported region", { region: region });
        res.sendStatus(404);
        return;
    }
    updateRegion(region, "tournament");  // fire away
    res.sendStatus(204);
});
// force an update
app.post("/api/player/:name/search", async (req, res) => {
    searchPlayer(req.params.name);  // do not await, just fire
    res.sendStatus(204);  // notifications will follow
});
// update a known user
// flow:
//   * get from db (depending on category, either regular, brawl or tourn)
//   * update lifetime from `/players` via id, catch IGN changes
//   * update from `/matches`
app.post("/api/player/:name/update/:category*?", async (req, res) => {
    const name = req.params.name, category = req.params.category || "regular",
        players = await model.Player.findAll({ where: { name: req.params.name } });
    if (players == undefined) {
        logger.error("player not found in db", { name: req.params.name });
        res.sendStatus(404);
        return;
    }
    logger.info("player in db, updating", { name: name, category: category });
    players.forEach((player) => updatePlayer(player, category));  // fire away
    res.sendStatus(204);
});
// crunch a known user  TODO force mode
app.post("/api/player/:name/crunch/:category*?", async (req, res) => {
    const category = req.params.category || "regular",
        players = await model.Player.findAll({ where: { name: req.params.name } });
    if (players == undefined) {
        logger.error("player not found in db, won't recrunch",
            { name: req.params.name });
        res.sendStatus(404);
        return;
    }
    logger.info("player in db, recrunching", { name: req.params.name });
    players.forEach((player) => crunchPlayer(category, player.api_id));  // fire away
    res.sendStatus(204);
});
// analyze a known user (calculate mmr)
app.post("/api/player/:name/rank", async (req, res) => {
    const players = await model.Player.findAll({ where: { name: req.params.name } });
    if (players == undefined) {
        logger.error("player not found in db, won't recrunch",
            { name: req.params.name });
        res.sendStatus(404);
        return;
    }
    logger.info("player in db, analyzing", { name: req.params.name });
    players.forEach((player) => analyzePlayer(player.api_id));  // fire away
    res.sendStatus(204);
});
// crunch a known team
app.post("/api/team/:id/crunch", async (req, res) => {
    if (await model.Team.findOne({ where: { id: req.params.id } }) == undefined) {
        logger.error("team not found in db, won't recrunch",
            { name: req.params.id });
        res.sendStatus(404);
        return;
    }
    logger.info("team in db, recrunching", { name: req.params.id });
    crunchTeam(req.params.id);  // fire away
    res.sendStatus(204);
});
// grab multiple users by IGN
app.post("/api/players/:region/grab/:category*?", async (req, res) => {
    let grabstart = new Date();
    grabstart.setTime(grabstart.getTime() - req.query.minutesAgo * 60000);
    grabPlayers(req.query.names, req.params.region, grabstart,
        req.params.category || "regular");
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
    logger.info("requesting download for Telemetry", { api_id: req.params.match });
    const asset = await model.Asset.findOne({ where: {
        match_api_id: req.params.match
    } });
    if (asset == undefined) {
        logger.error("could not find any assets for match",
            { api_id: req.params.match });
        res.sendStatus(404);
        return;
    }
    await ch.sendToQueue(SAMPLE_QUEUE, new Buffer(JSON.stringify(asset.url)), {
        persistent: true, type: "telemetry",
        headers: { match_api_id: req.params.match }
    });
    res.sendStatus(204);
});
// crunch all global stats
app.post("/api/crunch/:category*?", async (req, res) => {
    const category = req.params.category || "regular";
    crunchGlobal(category, false);  // fire away
    res.sendStatus(204);
});
// analyze *all* matches
app.post("/api/rank", async (req, res) => {
    analyzeGlobal();  // fire away
    res.sendStatus(204);
});
// force updating all stats
app.post("/api/recrunch/:category*?", async (req, res) => {
    const category = req.params.category || "regular";
    crunchGlobal(category, true);  // fire away
    res.sendStatus(204);
});
// update all matches for a Toornament
app.post("/api/toornament/:name/update", async (req, res) => {
});

/* internal monitoring */
app.get("/", (req, res) => {
    res.sendFile(__dirname + "/index.html");
});

process.on("unhandledRejection", err => {
    logger.error("Uncaught Promise Error:", err.stack);
});
