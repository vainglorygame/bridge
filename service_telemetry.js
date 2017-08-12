#!/usr/bin/env node
/* jshint esnext: true */
"use strict";

const Promise = require("bluebird"),
    Service = require("./service_skeleton.js");

const logger = global.logger,
    SAMPLE_QUEUE = process.env.SAMPLE_QUEUE || "telesuck",
    SAMPLE_TOURNAMENT_QUEUE = process.env.SAMPLE_TOURNAMENT_QUEUE || "telesuck";

module.exports = class Analyzer extends Service {
    constructor() {
        super();

        this.setTargets({
            "regular": SAMPLE_QUEUE,
            "tournament": SAMPLE_TOURNAMENT_QUEUE
        });

        this.setRoutes({
            "/api/match/:match/telemetry/:category?": async (req, res) => {
                logger.info("requesting download for Telemetry", { api_id: req.params.match });
                const category = req.params.category || "regular",
                    db = this.getDatabase(category),
                    asset = await db.Asset.findOne({ where: {
                        match_api_id: req.params.match
                    } });
                if (asset == undefined) {
                    logger.error("could not find any assets for match",
                        { api_id: req.params.match });
                    res.sendStatus(404);
                    return;
                }
                await this.forward(this.getTarget(category), asset.url, {
                    persistent: true,
                    headers: { match_api_id: req.params.match }
                });
                res.sendStatus(204);
            }
        });
    }
}
