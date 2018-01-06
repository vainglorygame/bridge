#!/usr/bin/env node
/* jshint esnext: true */
"use strict";
const amqp = require("amqplib"),
    Promise = require("bluebird"),
    winston = require("winston"),
    loggly = require("winston-loggly-bulk"),
    datadog = require("winston-datadog"),
    Seq = require("sequelize"),
    request = require("request-promise"),
    express = require("express"),
    http = require("http");

const PORT = parseInt(process.env.PORT) || 8880,
    DATABASE_URI = process.env.DATABASE_URI,
    DATABASE_BRAWL_URI = process.env.DATABASE_BRAWL_URI,
    DATABASE_TOURNAMENT_URI = process.env.DATABASE_TOURNAMENT_URI,
    RABBITMQ_URI = process.env.RABBITMQ_URI || "amqp://localhost",
    LOGGLY_TOKEN = process.env.LOGGLY_TOKEN,
    DATADOG_TOKEN = process.env.DATADOG_TOKEN,
    BRAWL = process.env.BRAWL != "false",
    TOURNAMENT = process.env.TOURNAMENT != "false";

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

global.logger = logger;

const Crunch = require("./service_crunch.js"),
    Analyze = require("./service_analyze.js"),
    Grab = require("./service_grab.js"),
    Telemetry = require("./service_telemetry.js");

let rabbit, ch,
    cruncher = new Crunch(), analyzer = new Analyze(),
    grabber = new Grab(), telemeter = new Telemetry(),
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

// datadog integration
if (DATADOG_TOKEN)
    logger.add(new datadog({
        api_key: DATADOG_TOKEN
    }), null, true);

// connect to broker and db
let dbs = {};
seq = new Seq(DATABASE_URI, { logging: false });
dbs.regular = model = require("../orm/model")(seq, Seq);
if (BRAWL) seqBrawl = new Seq(DATABASE_BRAWL_URI, { logging: false });
if (BRAWL) dbs.brawl = modelBrawl = require("../orm/model")(seqBrawl, Seq);
if (TOURNAMENT) seqTournament = new Seq(DATABASE_TOURNAMENT_URI, { logging: false });
if (TOURNAMENT) dbs.tournament = modelTournament = require("../orm/model")(seqTournament, Seq);

cruncher.setDatabases(dbs);
grabber.setDatabases(dbs);
analyzer.setDatabases(dbs);
telemeter.setDatabases(dbs);

amqp.connect(RABBITMQ_URI).then(async (rabbit) => {
    process.on("SIGINT", () => {
        rabbit.close();
        process.exit();
    });

    ch = await rabbit.createChannel();
    await cruncher.register(ch, app);
    await grabber.register(ch, app);
    await analyzer.register(ch, app);
    await telemeter.register(ch, app);
});

server.listen(PORT);
app.use(express.static("assets"));

/* internal monitoring */
app.get("/", (req, res) => res.sendFile(__dirname + "/index.html"));

process.on("unhandledRejection", (err) => {
    logger.error(err);
    process.exit(1);  // fail hard and die
});
