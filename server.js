#!/usr/bin/env node
/* jshint esnext: true */

var amqp = require("amqplib"),
    request = require("request-promise"),
    app = require("express")(),
    http = require("http").Server(app),
    sleep = require("sleep-promise");

var MADGLORY_TOKEN = process.env.MADGLORY_TOKEN,
    RABBITMQ_URI = process.env.RABBITMQ_URI || "amqp://localhost",
    REGIONS = ["na", "eu", "sg", "sa", "ea"];
if (MADGLORY_TOKEN == undefined) throw "Need an API token";

(async () => {
    var rabbit = await amqp.connect(RABBITMQ_URI),
        ch = await rabbit.createChannel();

    http.listen(8880);


    /* API helper */
    /* search for player name across all shards */
    async function api_playerByAttr(attr, val) {
        let finds = [],
            filter = "filter[" + attr + "]";

        for (let region of REGIONS) {
            let options = {
                uri: "https://api.dc01.gamelockerapp.com/shards/" + region + "/players",
                headers: {
                    "X-Title-Id": "semc-vainglory",
                    "Authorization": MADGLORY_TOKEN
                },
                qs: {},
                json: true,
                gzip: true
            };
            options.qs[filter] = val;
            retry = true;
            while (retry) {
                try {
                    res = await request(options);
                    finds.push({
                        "region": res.data[0].attributes.shardId,
                        "id": res.data[0].id,
                        "name": res.data[0].attributes.name,
                        "last_update": res.data[0].attributes.createdAt,
                        "source": "api"
                    });
                    retry = false;
                } catch (err) {
                    if (err.statusCode == 429) {
                        await sleep(100);
                        retry = true;
                    } else if (err.statusCode == 404) {
                        retry = false;
                    } else {
                        console.error(err);
                        retry = false;
                    }
                    // TODO
                }
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
        /*
        var player = await db_playerByName(name);
        if (player != undefined) return player;
        */

        console.log("player '" + name + "' not found in db");
        player = await api_playerByName(name);
        if (player != undefined) return player;

        console.log("player '" + name + "' not found in API");
        return undefined;
    }
    /* returns a player by id from db or API */
    async function playerById(id) {
        /*
        var player = await db_playerById(id);
        if (player != undefined) return player;
        */

        console.log("player with id '" + name + "' not found in db");
        player = await api_playerById(id);
        if (player != undefined) return player;

        console.log("player with id '" + name + "' not found in API");
        return undefined;
    }
    /* returns true if a player has pending jobs */
    async function anyJobsRunningFor(con, name, id) {
        return false;  // TODO
    }

    async function playerRequestUpdate(name, id) {
        if (name == undefined && id == undefined)  // fail hard and die
            throw "playerRequestUpdate needs either name or id";

        let player;
        if (id != undefined)  // prefer id over name
            player = await playerById(id);
        else
            player = await playerByName(name);
        if (player == undefined)
            return undefined;  // 404

        console.log("updating player '%j'", player);

        // if last_update is from our db, use it as a start, else get the whole history
        if (player.source == "api" || player.last_update == undefined)
            player.last_update = new Date(value=0);  // forever ago

        // add 1s, because createdAt-start <= x <= createdAt-end
        // so without the +1s, we'd always get the last_match_created_date match back
        player.last_update.setSeconds(player.last_update.getSeconds() + 1);

        var timedelta_minutes = ((new Date()) - player.last_update) / 1000 / 60;
        if (timedelta_minutes < 30) {
            console.log("player '" + player.name + "' update skipped");
            return player;
        }

        var payload = {
            "region": player.region,
            "params": {
                "filter[playerIds]": player.id,
                "filter[createdAt-start]": player.last_update.toISOString(),
                "filter[gameMode]": "casual,ranked"
            }
        };
        await ch.sendToQueue("grab", new Buffer(JSON.stringify(payload)), { persistent: true });

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


    //io.emit(name, "done");
})();
