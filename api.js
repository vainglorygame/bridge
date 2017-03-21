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
/* searches for player name across all shards */
async function findPlayer(name) {
    var regions = ["na", "eu", "sg"],
        finds = [];

    for (let region of regions) {
        var options = {
            uri: "https://api.dc01.gamelockerapp.com/shards/" + region + "/players",
            headers: {
                "X-Title-Id": "semc-vainglory",
                "Authorization": APITOKEN
            },
            qs: {
                "filter[playerNames]": name
            },
            json: true,
            gzip: true
        };
        try {
            res = await request(options);
            finds.push({
                "region": res.data[0].attributes.shardId,
                "id": res.data[0].id,
                "last_update": res.data[0].attributes.createdAt
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

/* routes */
app.get("/api/player/:name", async (req, res) => {
    var name = req.params.name;

    var raw = await pool_raw.connect(),
        web = await pool_web.connect();

    /* search for the player in our db */
    var player = await web.query(`
        SELECT api_id, shard_id, last_match_created_date
        FROM player WHERE name=$1
    `, [name]);
    var player_id, player_region, grab_start;

    /* found */
    if (player.rows.length > 0) {
        console.log("player '" + name + "' was found in db");
        player_id = player.rows[0].api_id;
        player_region = player.rows[0].shard_id;
        grab_start = player.rows[0].last_match_created_date;
        // TODO db does not save time zone offset @stormcaller remove this
        grab_start.setMinutes(grab_start.getMinutes() - (new Date().getTimezoneOffset()));
    }
    /* not found */
    if (player.rows.length == 0) {
        console.log("player '" + name + "' not found in db");
        /* search in all regions */
        player = await findPlayer(name);
        if (player == undefined) {
            console.log("player '" + name + "' not found in API");
            /* give up */
            res.sendStatus(404);
            return;
        }

        player_id = player.id;
        player_region = player.region;
        grab_start = new Date("2017-01-01T00:00:00Z");
    }

    /* request update job */
    // createdAt-start <= x <= createdAt-end
    grab_start.setSeconds(grab_start.getSeconds() + 1);
    payload = {
        "region": player_region,
        "params": {
            "filter[playerIds]": player_id,
            "filter[playerNames]": name, // TODO remove in 2.0 - backwards compat
            "filter[createdAt-start]": grab_start.toISOString(),
            "filter[gameMode]": "casual,ranked"
        }
    };
    var jobq = await raw.query(`
        INSERT INTO jobs(type, payload, priority)
        VALUES('grab', $1, 0)
        RETURNING id
    `, [payload]);
    console.log("player '" + name + "' updating after " + grab_start.toISOString());

    /* wake apigrabber up */
    await raw.query(`NOTIFY grab_open`, []);
    jobid = jobq.rows[0].id;

    /* clean up */
    raw.release();
    web.release();

    res.json({
        "job_id": jobid,
        "player_id": player_id,
        "player_region": player_region
    });
});

/* internal monitoring */
app.get("/", async (req, res) => {
    res.sendFile(__dirname + "/index.html");
});

/* notifications from database */
function listen() {
    var client = new pg.Client(db_config_raw);
    client.connect();
    client.on('notification', (msg) => {
        io.emit("job update", msg.channel);
    });
    client.query("LISTEN grab_open");
    client.query("LISTEN process_open");
    client.query("LISTEN compile_open");
    client.query("LISTEN analyze_open");
    client.query("LISTEN grab_running");
    client.query("LISTEN process_running");
    client.query("LISTEN compile_running");
    client.query("LISTEN analyze_running");
    client.query("LISTEN grab_finished");
    client.query("LISTEN process_finished");
    client.query("LISTEN compile_finished");
    client.query("LISTEN analyze_finished");
    client.query("LISTEN grab_failed");
    client.query("LISTEN process_failed");
    client.query("LISTEN compile_failed");
    client.query("LISTEN analyze_failed");
    // keep open forever
}

listen();
