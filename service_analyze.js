#!/usr/bin/env node
/* jshint esnext: true */
"use strict";

const Promise = require("bluebird"),
    Service = require("./service_skeleton.js");

const logger = global.logger,
    ANALYZE_QUEUE = process.env.ANALYZE_QUEUE || "analyze",
    ANALYZE_TOURNAMENT_QUEUE = process.env.ANALYZE_TOURNAMENT_QUEUE || "analyze_tournament",
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

    async analyzeGlobal(category) {
        const db = this.getDatabase(category);
        let offset = 0, matches;
        do {
            matches = await db.Match.findAll({
                attributes: ["api_id"],
                where: { trueskill_quality: null },
                limit: SHOVEL_SIZE,
                offset: offset,
                order: [ ["created_at", "ASC"] ]
            });
            await Promise.each(matches, async (m) =>
                await this.forward(this.getTarget(category), m.api_id,
                    { persistent: true }));
            offset += SHOVEL_SIZE;
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
