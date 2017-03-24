#!/usr/bin/env node
/* jshint esnext: true */

var request = require("request-promise");

var pg = require("pg");
var Pool = pg.Pool;
var db_config_raw = {
    user: "vainraw",
    password: "vainraw",
    host: "localhost",
    database: "vainsocial-raw",
    port: 5433,
    max: 10
};
var db_config_web = {
    user: "vainweb",
    password: "vainweb",
    host: "localhost",
    database: "vainsocial-web",
    port: 5432,
    max: 10
};
var pool_raw = new Pool(db_config_raw);
var pool_web = new Pool(db_config_web);

var app = require("express")();
var http = require("http").Server(app);
var io = require("socket.io")(http);

var APITOKEN = process.env.VAINSOCIAL_APITOKEN;
if (APITOKEN == undefined) throw "Need a valid API token!";

http.listen(8080);

/* API helper */
/* search for player name across all shards */
async function api_playerByAttr(attr, val) {
    var regions = ["na", "eu", "sg"],
        finds = [],
        filter = "filter[" + attr + "]";

    for (let region of regions) {
        var options = {
            uri: "https://api.dc01.gamelockerapp.com/shards/" + region + "/players",
            headers: {
                "X-Title-Id": "semc-vainglory",
                "Authorization": APITOKEN
            },
            qs: {
                filter: val
            },
            json: true,
            gzip: true
        };
        try {
            res = await request(options);
            finds.push({
                "region": res.data[0].attributes.shardId,
                "id": res.data[0].id,
                "last_update": res.data[0].attributes.createdAt,
                "source": "api"
            });
        } catch (err) {
            // TODO
        }
    }

    if (finds.length == 0)
        return undefined;

    // due to an API bug, many players are also present in NA
    // TODO: get history for all regions in case of region transfer
    finds.sort((a, b) => { return a.last_update < b.last_update; });
    return finds[0];
}
async function api_playerByName(name) {
    return await api_playerByAttr("playerNames", name);
}
async function api_playerById(id) {
    return await api_playerByAttr("playerIds", id);
}


/* DB helper */
/* retry until no serialization error */
async function db_serialized(con, query, data) {
    var commit_success = false,
        res;
    do {
        /* transaction begin */
        try {
            await con.query("BEGIN");
            await con.query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE");
            res = await con.query(query, data);
            await con.query("COMMIT");
            commit_success = true;
        } catch (err) {
            await con.query("ROLLBACK");
            // serialization error - expected, else rethrow
            console.error(err);
            if (err.code != "40001") throw err;
        }
        /* transaction end */
    } while (!commit_success);
    return res;
}

/* find player by id or by name in db */
async function db_playerByAttr(attr, val) {
    var web = await pool_web.connect(),
        player = await web.query(`
            SELECT name, api_id, shard_id, last_match_created_date
            FROM player WHERE ` + attr + `=$1
        `, [val]);
    web.release();

    if (player.rows.length > 0) {
        return {
            "name": player.rows[0].name,
            "id": player.rows[0].api_id,
            "region": player.rows[0].shard_id,
            "last_update": player.rows[0].last_match_created_date,
            "source": "db"
        };
    }
    return undefined;
}

async function db_playerByName(name) {
    return await db_playerByAttr("name", name);
}
async function db_playerById(id) {
    return await db_playerByAttr("api_id", id);
}

/* returns a player by name from db or API */
async function playerByName(name) {
    var player = await db_playerByName(name);
    if (player != undefined) return player;

    console.log("player '" + name + "' not found in db");
    player = await api_playerByName(name);
    if (player != undefined) return player;

    console.log("player '" + name + "' not found in API");
    return undefined;
}
/* returns a player by id from db or API */
async function playerById(id) {
    var player = await db_playerById(id);
    if (player != undefined) return player;

    console.log("player with id '" + name + "' not found in db");
    player = await api_playerById(id);
    if (player != undefined) return player;

    console.log("player with id '" + name + "' not found in API");
    return undefined;
}


/* update request helpers */
/* upsert a job */
async function upsertGrabjob(payload) {
    var raw = await pool_raw.connect(),
        job;

    // find and prioritize existing jobs
    job = await db_serialized(raw, `
        UPDATE jobs SET priority=0
        WHERE
        (
            (type='grab' AND payload=$1) OR
            (type='process' AND payload->>'playername'=$1->'params'->>'filter[playerNames]') OR
            (type='compile' AND payload->>'type'='player' AND payload->>'id'=$1->'params'->>'filter[playerIds]')
        ) AND status<>'finished' AND status<>'failed'
        RETURNING id
    `, [payload]);
    // TODO job dependency information format on jobs is shit
    // (2.0)

    if (job.rows.length == 0) {
        // this job is currently not running, insert it
        job = await db_serialized(raw, `
            INSERT INTO jobs(type, payload, priority)
            VALUES('grab', $1, 0)
            RETURNING id
        `, [payload]);
        // wake apigrabber up
        await raw.query(`NOTIFY grab_open`);
        console.log("job requested: '%j'", payload);
    }

    raw.release();
    return job.rows[0];
}

async function playerRequestUpdate(name, id) {
    if (name == undefined && id == undefined)  // fail hard and die
        throw "playerRequestUpdate needs either name or id";

    var player;
    if (id != undefined)  // prefer id over name
        player = await playerById(id);
    else
        player = await playerByName(name);
    if (player == undefined)
        return undefined;

    console.log("updating player '%j'", player);

    // if last_update is from our db, use it as a start, else get the whole history
    if (player.source == "api" || player.last_update == undefined)
        player.last_update = new Date(value=0);  // forever ago
    /* comment out on shutter's machine
    if (player.source == "db")
        player.last_update.setMinutes(player.last_update.getMinutes() - new Date().getTimezoneOffset());  // TODO workaround for my broken db schema
    */

    // add 1s, because createdAt-start <= x <= createdAt-end
    // so without the +1s, we'd always get the last_match_created_date match back
    player.last_update.setSeconds(player.last_update.getSeconds() + 1);

    var timedelta_minutes = ((new Date()) - player.last_update) / 1000 / 60;
    if (timedelta_minutes < 30) {
        console.log("player '" + player.name + "' update skipped");
        return;
    }

    var payload = {
        "region": player.region,
        "params": {
            "filter[playerIds]": player.id,
            "filter[playerNames]": player.name, // TODO remove in 2.0 - backwards compat
            "filter[createdAt-start]": player.last_update.toISOString(),
            "filter[gameMode]": "casual,ranked"
        }
    };
    job = await upsertGrabjob(payload);

    return player;
}
async function playerRequestUpdateByName(name) {
    return await playerRequestUpdate(name, undefined);
}
async function playerRequestUpdateById(id) {
    return await playerRequestUpdate(undefined, id);
}

/* routes */
/* request a grab job */
app.get("/api/player/name/:name", async (req, res) => {
    player = await playerRequestUpdateByName(req.params.name);
    if (player == undefined) res.sendStatus(404);
    else res.json(player);
});
app.get("/api/player/id/:id", async (req, res) => {
    player = await playerRequestUpdateById(req.params.id);
    if (player == undefined) res.sendStatus(404);
    else res.json(player);
});

/* internal monitoring */
app.get("/", async (req, res) => {
    res.sendFile(__dirname + "/index.html");
});


/* notifications from database */
async function listen() {
    var client = new pg.Client(db_config_raw);
    await client.connect();

    /* job status change notification listener */
    client.on('notification', async (msg) => {
        var raw = await pool_raw.connect(),
            jobs;

        // find all interesting jobs, delete them & forward their notification
        if (msg.channel == "grab_failed") {
            jobs = await raw.query(`
                DELETE FROM jobs WHERE
                type='grab' AND status='failed' AND payload->>'error'='Not Found'
                RETURNING
                payload->'params'->>'filter[playerIds]' AS player_id,
                payload->'params'->>'filter[playerNames]' AS player_name
            `);
        }
        if (msg.channel == "process_finished") {
            jobs = await raw.query(`
                DELETE FROM jobs WHERE
                type='process' AND status='finished'
                RETURNING payload->>'playername' AS player_name
            `);
        }
        if (msg.channel == "compile_finished") {
            jobs = await raw.query(`
                DELETE FROM jobs WHERE
                type='compile' AND payload->>'type'='player' AND status='finished'
                RETURNING payload->>'id' AS player_id
            `);
        }

        raw.release();
        if (jobs == undefined) return;  // nothing to do

        // forward notification to all playername / playerid channels
        for (let job of jobs.rows) {
            var name = job.player_name;
            var id = job.player_id;
            var player;
            if (name == undefined && id == undefined) throw "notification needs either name or ID";
            if (name == undefined)
                player = await db_playerById(job.player_id);
            else
                player = await db_playerByName(job.player_name);
            if (player == undefined) throw "player had a job, but doesn't exist";
            console.log("sending '%s' notification for player '%s'", msg.channel, player.name);
            io.emit(player.name, msg.channel);
            io.emit(player.id, msg.channel);
        }
    });
    client.query("LISTEN process_finished");
    client.query("LISTEN compile_finished");
    client.query("LISTEN analyze_finished");
    client.query("LISTEN grab_failed");
    // keep open forever
}

listen();
