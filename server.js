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

const PORT = parseInt(process.env.PORT) || 8880,
    KEY_TYPE = process.env.KEY_TYPE || "crunch",  // backwards compat
    DATABASE_URI = process.env.DATABASE_URI,
    DATABASE_BRAWL_URI = process.env.DATABASE_BRAWL_URI,
    DATABASE_TOURNAMENT_URI = process.env.DATABASE_TOURNAMENT_URI,
    RABBITMQ_URI = process.env.RABBITMQ_URI || "amqp://localhost",
    LOGGLY_TOKEN = process.env.LOGGLY_TOKEN,
    BRAWL = process.env.BRAWL != "false",
    TOURNAMENT = process.env.TOURNAMENT != "false",
    REGIONS = (process.env.REGIONS || "na,eu,sg,sa,ea").split(","),
    TOURNAMENT_REGIONS = (process.env.TOURNAMENT_REGIONS || "tournament-na," +
        "tournament-eu,tournament-sg,tournament-sa,tournament-ea").split(","),
    GRABSTART = process.env.GRABSTART || "2017-02-14T00:00:00Z",
    BRAWL_RETENTION_DAYS = parseInt(process.env.BRAWL_RETENTION_DAYS) || 3,
    PLAYER_PROCESS_QUEUE = process.env.PLAYER_PROCESS_QUEUE || "process",
    PLAYER_TOURNAMENT_PROCESS_QUEUE = process.env.PLAYER_TOURNAMENT_PROCESS_QUEUE || "process_tournament",
    GRAB_QUEUE = process.env.GRAB_QUEUE || "grab",
    GRAB_BRAWL_QUEUE = process.env.GRAB_BRAWL_QUEUE || "grab_brawl",
    GRAB_TOURNAMENT_QUEUE = process.env.GRAB_TOURNAMENT_QUEUE || "grab_tournament",
    CRUNCH_QUEUE = process.env.CRUNCH_QUEUE || "crunch",
    CRUNCH_TOURNAMENT_QUEUE = process.env.CRUNCH_TOURNAMENT_QUEUE || "crunch_tournament",
    ANALYZE_QUEUE = process.env.ANALYZE_QUEUE || "analyze",
    ANALYZE_TOURNAMENT_QUEUE = process.env.ANALYZE_TOURNAMENT_QUEUE || "analyze_tournament",
    SAMPLE_QUEUE = process.env.SAMPLE_QUEUE || "sample",
    SAMPLE_TOURNAMENT_QUEUE = process.env.SAMPLE_TOURNAMENT_QUEUE || "sample_tournament",
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
            await ch.assertQueue(PLAYER_TOURNAMENT_PROCESS_QUEUE, {durable: true});
            await ch.assertQueue(CRUNCH_QUEUE, {durable: true});
            await ch.assertQueue(CRUNCH_TOURNAMENT_QUEUE, {durable: true});
            await ch.assertQueue(SAMPLE_QUEUE, {durable: true});
            await ch.assertQueue(SAMPLE_TOURNAMENT_QUEUE, {durable: true});
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

server.listen(PORT);
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

function sampleQueueForCategory(category) {
    switch (category) {
        case "regular": return SAMPLE_QUEUE;
        case "tournament": return SAMPLE_TOURNAMENT_QUEUE;
        default: logger.error("unsupported game mode category",
            { category: category });
    }
}

function processQueueForCategory(category) {
    switch (category) {
        case "regular": return PLAYER_PROCESS_QUEUE;
        case "tournament": return PLAYER_TOURNAMENT_PROCESS_QUEUE;
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

function analyzeQueueForCategory(category) {
    switch (category) {
        case "regular": return ANALYZE_QUEUE;
        case "tournament": return ANALYZE_TOURNAMENT_QUEUE;
        default: logger.error("unsupported game mode category",
            { category: category });
    }
}

// API requires maximum time interval of 4w
// return [payload, payload, …] with fixed createdAt
function splitGrabs(payload, start, end) {
    let part_start = start,
        part_end = start,
        payloads = [];
    // loop forwards, adding 4w until we passed NOW
    while (part_start < end) {
        part_end = new Date(part_end.getTime() + (4 * 60*60*24*7*1000));  // add 4w
        if(part_end > end) part_end = end;
        let pl = payload;
        pl["params"]["filter[createdAt-start]"] = part_start.toISOString();
        pl["params"]["filter[createdAt-end]"] = part_end.toISOString();
        // push a deep clone or the object will have the reference to part_start
        // which leads to all start/end dates being the same (?!)
        payloads.push(JSON.parse(JSON.stringify(pl)));  // JavaScript sucks.
        part_start = part_end;
    }
    return payloads;
}

// request grab jobs for a player's matches
async function grabPlayer(name, region, last_match_created_date, id, category) {
    const start = new Date(Date.parse(last_match_created_date)),
        end = new Date();  // today

    const payload_template = {
        "region": region,
        "params": {
            "filter[playerIds]": id,
            "filter[gameMode]": gameModesForCategory(category),
            "sort": "createdAt"
        }
    };
    await Promise.each(splitGrabs(payload_template, start, end), async (payload) => {
        logger.info("requesting update", {
            name: name,
            region: region
        });

        await ch.sendToQueue(grabQueueForCategory(category),
                new Buffer(JSON.stringify(payload)), {
            persistent: true,
            type: "matches",
            headers: {
                donotify: true,  // TODO backwards compat
                notify: "player." + name
            }
        });
    });
}
// request grab job for multiple players' matches
async function grabPlayers(names, region, grabstart, category) {
    // TODO MadGlory change, call API in 4w distances
    const payload_template = {
        "region": region,
        "params": {
            "filter[playerNames]": names,
            "filter[gameMode]": gameModesForCategory(category),
            "sort": "createdAt"
        }
    }, now = new Date();
    logger.info("requesting triple player grab", {
        names: names,
        region: region,
        category: category
    });

    await Promise.each(splitGrabs(payload_template, grabstart, now), async (payload) => {
        await ch.sendToQueue(grabQueueForCategory(category),
                new Buffer(JSON.stringify(payload)), {
            persistent: true,
            type: "matches",
            headers: {
                donotify: true,  // TODO
                notify: "player." + names
            }
        });
    });
}

// request grab jobs for a region's matches
async function grabMatches(region, last_match_created_date) {
    last_match_created_date = last_match_created_date
        || new Date(Date.parse(defaultGrabstartForCategory("tournament")));
    const now = new Date();

    // add 1s, because createdAt-start <= x <= createdAt-end
    // so without the +1s, we'd always get the last_match_created_date match back
    last_match_created_date.setSeconds(last_match_created_date.getSeconds() + 1);

    const payload_template = {
        "region": region,
        "params": {
            "sort": "createdAt",
            "page[limit]": 5  // TODO maximum for `/matches`
        }
    };
    logger.info("requesting region update", { region: region });

    await Promise.each(splitGrabs(payload_template, last_match_created_date, now), async (payload) => {
        await ch.sendToQueue(GRAB_TOURNAMENT_QUEUE, new Buffer(JSON.stringify(payload)), {
            persistent: true,
            type: "matches"
        });
    });
}

// search for a player name in one region
// request process for lifetime
// return an array (JSONAPI response)
async function searchPlayerInRegion(region, name, id, category) {
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
            await ch.sendToQueue(processQueueForCategory(category),
                new Buffer(JSON.stringify(player)), {
                    persistent: true, type: "player",
                    headers: {
                        donotify: true,  // TODO
                        notify: "player." + player.name
                    }
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
async function searchPlayer(name, category) {
    let found = false;
    logger.info("searching", { name: name });
    await Promise.map(regionsForCategory(category), async (region) => {
        const players = await searchPlayerInRegion(region, name, undefined, category);
        if (players.length > 0) found = true;

        // request grab jobs
        await Promise.map(players, (p) =>
            grabPlayer(p.name, p.shardId,
                defaultGrabstartForCategory(category), p.id, category));
    });
    // notify web
    if (!found) {
        logger.info("search failed", { name: name });
        await ch.publish("amq.topic", "player." + name,
            new Buffer("search_fail"));
    }
}

// return REGIONS based on category
function regionsForCategory(category) {
    if (category == "regular")
        return REGIONS;
    if (category == "tournament")
        return TOURNAMENT_REGIONS;
    logger.error("unsupported category", { category: category });
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
        return "2017-02-12T00:00:00Z";  // all
    if (category == "regular")
        return GRABSTART;  // as via env var
}

// update a player from API based on db record
// & grab player
async function updatePlayer(player, category) {
    // set last_update and request an update job
    // if last_update is null, we need that player's full history
    let grabstart;
    if (player.get("last_update") != null && category == "regular")
        // TODO do not fetch everything on category tournament / brawl
        grabstart = player.get("created_at");
    if (grabstart == undefined) grabstart = defaultGrabstartForCategory(category);
    else
        // add 1s, because createdAt-start <= x <= createdAt-end
        // so without the +1s, we'd always get the last_match_created_date match back
        grabstart.setSeconds(grabstart.getSeconds() + 1);

    const players = await searchPlayerInRegion(
        player.get("shard_id"), player.get("name"), player.get("api_id"), category);

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
    const last_update = await getKey(model,
        `samples_last_update+${region}`,
        (new Date(0)).toISOString());

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
    await setKey(model, `samples_last_update+${region}`,
        region, last_update);
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
async function analyzePlayer(category, api_id) {
    const db = databaseForCategory(category),
        participations = await db.Participant.findAll({
        attributes: ["match_api_id"],
        where: {
            player_api_id: api_id,
            trueskill_mu: null  // where not analyzed yet
        },
        order: [ ["created_at", "ASC"] ]
    });
    logger.info("sending matches to analyzer",
        { length: participations.length });
    await Promise.each(participations, async (p) =>
        await ch.sendToQueue(analyzeQueueForCategory(category),
            new Buffer(p.match_api_id),
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
        "global_last_crunch_participant_id", 0);

    if (force) {
        // refresh everything
        logger.info("deleting all global points");
        await db.GlobalPoint.destroy({ truncate: true });
        last_crunch_participant_id = await setKey(db,
            "global_last_crunch_participant_id", 0);
    }

    // don't load the whole Participant table at once into memory
    let participations;

    logger.info("loading all participations into cruncher",
        { last_crunch_participant_id: last_crunch_participant_id });
    do {
        participations = await db.Participant.findAll({
            attributes: ["api_id", "id"],
            where: {
                id: { $gt: last_crunch_participant_id }
            },
            limit: SHOVEL_SIZE,
            order: [ ["id", "ASC"] ]
        });
        await Promise.map(participations, async (p) =>
            await ch.sendToQueue(crunchQueueForCategory(category),
                new Buffer(p.api_id),
                { persistent: true, type: "global" }));

        // update lpcid & refetch
        if (participations.length > 0) {
            last_crunch_participant_id = participations[participations.length-1].id;
            await setKey(db, "global_last_crunch_participant_id",
                last_crunch_participant_id);
        }
        logger.info("loading more participations into cruncher", {
            limit: SHOVEL_SIZE,
            size: participations.length,
            last_crunch_participant_id: last_crunch_participant_id
        });
    } while (participations.length == SHOVEL_SIZE);
    logger.info("done loading participations into cruncher");
}

// upanalyze all matches  TODO add force option
async function analyzeGlobal(category) {
    const db = databaseForCategory(category);
    let offset = 0, matches;
    do {
        matches = await db.Match.findAll({
            attributes: ["api_id"],
            where: {
                trueskill_quality: null
            },
            limit: SHOVEL_SIZE,
            offset: offset,
            order: [ ["created_at", "ASC"] ]
        });
        await Promise.each(matches, async (m) =>
            await ch.sendToQueue(analyzeQueueForCategory(category),
                new Buffer(m.api_id),
                { persistent: true }));
        offset += SHOVEL_SIZE;
        logger.info("loading more matches into analyzer",
            { offset: offset, limit: SHOVEL_SIZE, size: matches.length });
    } while (matches.length == SHOVEL_SIZE);
    logger.info("done loading matches into analyzer");
}

// return an entry from keys db
async function getKey(db, key, default_value) {
    const record = await db.Keys.findOrCreate({
        where: {
            type: KEY_TYPE,
            key: key
        },
        defaults: { value: default_value }
    });
    return record[0].value;
}

// update an entry from keys db
async function setKey(db, key, value) {
    const record = await db.Keys.findOrCreate({
        where: {
            type: KEY_TYPE,
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
        logger.error("called with unsupported region", { region: region });
        res.sendStatus(404);
        return;
    }
    updateRegion(region, "tournament");  // fire away
    res.sendStatus(204);
});
// force an update
app.post("/api/player/:name/search/:category*?", async (req, res) => {
    const name = req.params.name, category = req.params.category || "regular";
    searchPlayer(name, category);  // do not await, just fire
    res.sendStatus(204);  // notifications will follow
});
// update a known user
// flow:
//   * get from db (depending on category, either regular, brawl or tourn)
//   * update lifetime from `/players` via id, catch IGN changes
//   * update from `/matches`
app.post("/api/player/:name/update/:category*?", async (req, res) => {
    const name = req.params.name,
        category = req.params.category || "regular",
        db = databaseForCategory(category),
        players = await db.Player.findAll({ where: { name: req.params.name } });
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
        db = databaseForCategory(category),
        players = await db.Player.findAll({ where: { name: req.params.name } });
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
app.post("/api/player/:name/rank/:category*?", async (req, res) => {
    const category = req.params.category || "regular",
        db = databaseForCategory(category),
        players = await db.Player.findAll({ where: { name: req.params.name } });
    if (players == undefined) {
        logger.error("player not found in db, won't analyze",
            { name: req.params.name });
        res.sendStatus(404);
        return;
    }
    logger.info("player in db, analyzing", { name: req.params.name });
    players.forEach((player) => analyzePlayer(category, player.api_id));  // fire away
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
app.post("/api/match/:match/telemetry/:category?", async (req, res) => {
    logger.info("requesting download for Telemetry", { api_id: req.params.match });
    const category = req.params.category || "regular",
        db = databaseForCategory(category),
        asset = await db.Asset.findOne({ where: {
            match_api_id: req.params.match
        } });
    if (asset == undefined) {
        logger.error("could not find any assets for match",
            { api_id: req.params.match });
        res.sendStatus(404);
        return;
    }
    await ch.sendToQueue(sampleQueueForCategory(category), new Buffer(JSON.stringify(asset.url)), {
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
app.post("/api/rank/:category*?", async (req, res) => {
    const category = req.params.category || "regular";
    analyzeGlobal(category);  // fire away
    res.sendStatus(204);
});
// force updating all stats
app.post("/api/recrunch/:category*?", async (req, res) => {
    const category = req.params.category || "regular";
    crunchGlobal(category, true);  // fire away
    res.sendStatus(204);
});

/* internal monitoring */
app.get("/", (req, res) => {
    res.sendFile(__dirname + "/index.html");
});

process.on("unhandledRejection", err => {
    logger.error("Uncaught Promise Error:", err.stack);
});
