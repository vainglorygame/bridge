#!/usr/bin/env node
/* jshint esnext: true */

var amqp = require("amqplib"),
    Seq = require("sequelize"),
    request = require("request-promise"),
    express = require("express"),
    http = require("http"),
    sleep = require("sleep-promise");

var MADGLORY_TOKEN = process.env.MADGLORY_TOKEN,
    DATABASE_URI = process.env.DATABASE_URI,
    RABBITMQ_URI = process.env.RABBITMQ_URI || "amqp://localhost",
    REGIONS = ["na", "eu", "sg", "sa", "ea"];
if (MADGLORY_TOKEN == undefined) throw "Need an API token";

var rabbit, ch, seq,
    app = express(),
    server = http.Server(app);

// connect to broker, retrying forever
(async () => {
    while (true) {
        try {
            seq = new Seq(DATABASE_URI, { logging: () => {} });
            rabbit = await amqp.connect(RABBITMQ_URI);
            ch = await rabbit.createChannel();
            await ch.assertQueue("grab", {durable: true});
            await ch.assertQueue("process", {durable: true});
            break;
        } catch (err) {
            console.error(err);
            await sleep(5000);
        }
    }
    model = require("../orm/model")(seq, Seq);
})();

server.listen(8880);
app.use(express.static("assets"));

// request a grab job
function requestUpdate(name, region, last_match_created_date, id) {
    last_match_created_date = last_match_created_date || new Date(value=0);

    // add 1s, because createdAt-start <= x <= createdAt-end
    // so without the +1s, we'd always get the last_match_created_date match back
    last_match_created_date.setSeconds(last_match_created_date.getSeconds() + 1);

    let payload = {
        "region": region,
        "params": {
            "filter[playerIds]": id,
            "filter[createdAt-start]": last_match_created_date.toISOString(),
            "filter[gameMode]": "casual,ranked",
            "sort": "-createdAt"
        }
    };
    console.log("requesting update for", name, region);
    return ch.sendToQueue("grab", new Buffer(JSON.stringify(payload)),
        { persistent: true });
}

// search for player name on all shards
// send a notification for results and request updates
async function searchPlayer(name) {
    let found = false;
    console.log("looking up", name);
    await Promise.all(REGIONS.map(async (region) => {
        let players = [];
        while (true) {
            console.log("looking up", name, region);
            try {
                // find players by name
                players = await request({
                    uri: "https://api.dc01.gamelockerapp.com/shards/" + region + "/players",
                    headers: {
                        "X-Title-Id": "semc-vainglory",
                        "Authorization": MADGLORY_TOKEN
                    },
                    qs: {
                        "filter[playerNames]": name
                    },
                    json: true,
                    gzip: true
                });
                console.log("found", name, region);
                break;
            } catch (err) {
                if (err.statusCode == 429) {
                    console.log("rate limited, sleeping");
                    await sleep(100);  // no return, no break => retry
                } else if (err.statusCode != 404) console.error(err);
                if (err.statusCode != 429) {
                    console.log("failed", name, region, err.statusCode);
                    return;
                }
            }
        }
        // players.length will be 1 in 99.9% of all cases
        // - but this will cover the 0.01% too
        //
        // send to processor, so the player is in db
        // no matter whether we find matches or not
        await ch.sendToQueue("process", new Buffer(JSON.stringify(players.data)),
            { persistent: true, type: "player" });

        // request grab jobs
        await Promise.all(players.data.map((p) =>
            requestUpdate(p.attributes.name, p.attributes.shardId, undefined, p.id)));

        found = true;
    }));
    // notify web
    if (found)
        await ch.publish("amq.topic", "player." + name,
            new Buffer("search_success"));
    else
        await ch.publish("amq.topic", "player." + name,
            new Buffer("search_fail"));
}

// update a player based on db record
function updatePlayer(player) {
    player.last_update = new Date();
    // set last_update and request an update job
    return Promise.all([
        player.save(),
        requestUpdate(player.name, player.shard_id,
            player.last_match_created_date, player.api_id)
    ]);
}


/* routes */
// force an update
app.post("/api/player/:name/search", (req, res) => {
    searchPlayer(req.params.name);  // do not await, just fire
    res.sendStatus(204);  // notifications will follow
});
// update a known user
app.post("/api/player/:name/update", async (req, res) => {
    let player = await model.Player.findOne({ where: { name: req.params.name } });
    if (player == undefined) {
        console.log("player not found in db, searching instead", req.params.name);
        await searchPlayer(req.params.name);
        return;
    }
    console.log("player in db, updating", req.params.name);
    await updatePlayer(player);
    res.sendStatus(204);
});
// update a random user
app.post("/api/player", async (req, res) => {
    let player = await model.Player.findOne({ order: [ Seq.fn("RAND") ] });
    await updatePlayer(player);
    res.json(player);
});

/* internal monitoring */
app.get("/", (req, res) => {
    res.sendFile(__dirname + "/index.html");
});
