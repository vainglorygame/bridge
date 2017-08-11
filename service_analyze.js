#!/usr/bin/env node
/* jshint esnext: true */
"use strict";

const Promise = require("bluebird"),
    Service = require("./service_skeleton.js");

const logger = global.logger,
    ANALYZE_QUEUE = process.env.ANALYZE_QUEUE || "analyze",
    ANALYZE_TOURNAMENT_QUEUE = process.env.ANALYZE_TOURNAMENT_QUEUE || "analyze_tournament",
    ANALYZE_MODES = (process.env.ANALYZE_MODES || "casual,ranked").split(","),
    SHOVEL_SIZE = parseInt(process.env.SHOVEL_SIZE) || 1000;

module.exports = class Analyzer extends Service {
    constructor() {
        super();

        this.setTargets({
            "regular": ANALYZE_QUEUE,
            "tournament": ANALYZE_TOURNAMENT_QUEUE
        });

        this.setRoutes({
            // calculate TrueSkill for everyone
            "/api/rank/:category*?": async (req, res) => {
                this.analyzeGlobal(req.params.category || "regular");
                res.sendStatus(204);
            },
            // calculate TrueSkill for everyone, overwriting everything
            "/api/rerank/:category*?": async (req, res) => {
                this.analyzeGlobal(req.params.category || "regular", true);
                res.sendStatus(204);
            },
            // calculate TrueSkill for one player
            "/api/player/:name/rank/:category*?": async (req, res) => {
                const category = req.params.category || "regular",
                    db = this.getDatabase(category),
                    players = await db.Player.findAll({ where: { name: req.params.name } });
                if (players == undefined) {
                    logger.error("player not found in db, won't analyze",
                        { name: req.params.name });
                    res.sendStatus(404);
                    return;
                }
                logger.info("player in db, analyzing", { name: req.params.name });
                players.forEach((player) => this.analyzePlayer(category, player.api_id));
                res.sendStatus(204);
            }
        });
    }

    // overwrite: analyze *all* matches
    async analyzeGlobal(category, overwrite=false) {
        const db = this.getDatabase(category);
        let offset = new Date(0), matches;
        do {
            matches = await db.Match.findAll({
                attributes: [ "api_id", "created_at" ],
                where: overwrite? {
                    "created_at": { $gt: offset },
                    "game_mode": { $in: ANALYZE_MODES }
                } : {
                    trueskill_quality: null,
                    "created_at": { $gt: offset },
                    "game_mode": { $in: ANALYZE_MODES }
                },
                limit: SHOVEL_SIZE,
                order: [ ["created_at", "ASC"] ]
            });
            if (matches.length > 0)
                offset = matches[matches.length-1].created_at;

            await Promise.each(matches, async (m) =>
                await this.forward(this.getTarget(category), m.api_id,
                    { persistent: true }));
            logger.info("loading more matches into analyzer",
                { offset: offset, limit: SHOVEL_SIZE, size: matches.length });
        } while (matches.length == SHOVEL_SIZE);
        logger.info("done loading matches into analyzer");
    }

    async analyzePlayer(category, api_id) {
        const db = this.getDatabase(category),
            participations = await db.Participant.findAll({
            attributes: ["match_api_id"],
            where: {
                // TODO filter for ANALYZE_MODES
                player_api_id: api_id,
                trueskill_mu: null  // where not analyzed yet
            },
            order: [ ["created_at", "ASC"] ]
        });
        logger.info("sending matches to analyzer",
            { length: participations.length });
        await Promise.each(participations, async (p) =>
            await this.forward(this.getTarget(category),
                p.match_api_id, { persistent: true }));
    }
}
