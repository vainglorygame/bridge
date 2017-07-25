#!/usr/bin/env node
/* jshint esnext: true */
"use strict";

const Promise = require("bluebird"),
    Service = require("./service_skeleton.js");

const logger = global.logger,
    CRUNCH_QUEUE = process.env.CRUNCH_QUEUE || "crunch_global",
    CRUNCH_PLAYER_QUEUE = process.env.CRUNCH_PLAYER_QUEUE || "crunch_player",
    CRUNCH_TOURNAMENT_QUEUE = process.env.CRUNCH_TOURNAMENT_QUEUE || "crunch_tournament",
    SHOVEL_SIZE = parseInt(process.env.SHOVEL_SIZE) || 1000;

module.exports = class Cruncher extends Service {
    constructor() {
        super();

        this.setTargets({
            "regular": CRUNCH_QUEUE,
            "regular_player": CRUNCH_PLAYER_QUEUE,
            "tournament": CRUNCH_TOURNAMENT_QUEUE
        });

        this.setRoutes({
            // crunch global meta
            "/api/crunch/:category*?": async (req, res) => {
                this.crunchGlobal(req.params.category || "regular");
                res.sendStatus(204);
            },
            // crunch all players
            "/api/player/crunch/:category*?": async (req, res) => {
                this.crunchGlobal(req.params.category || "regular", true);
                res.sendStatus(204);
            },
            // crunch a player
            "/api/player/:name/crunch/:category*?": async (req, res) => {
                const category = req.params.category || "regular",
                    db = this.getDatabase(req.params.category || "regular"),
                    players = await db.Player.findAll({ where: { name: req.params.name } });
                if (players == undefined) {
                    logger.error("player not found in db, won't crunch",
                        { name: req.params.name });
                    res.sendStatus(404);
                    return;
                }
                logger.info("player in db, crunching", { name: req.params.name });
                players.forEach((player) =>
                    this.crunchPlayer(category, player.api_id));  // fire away
                res.sendStatus(204);
            }
        });
    }

    // upcrunch player's stats
    async crunchPlayer(category, api_id) {
        const db = this.getDatabase(category),
            where = { player_api_id: api_id };

        // wipe previous calculations
        await db.PlayerPoint.destroy({ where });
        // get all participants for this player
        const participations = await db.Participant.findAll({
            attributes: [ "api_id" ],
            where
        });
        // send everything to cruncher
        logger.info("sending participations to cruncher",
            { length: participations.length });
        await Promise.map(participations, async (p) =>
            await this.forward(this.getTarget(category + "_player"),
                p.api_id, { persistent: true }));
    }

    // crunch global stats
    async crunchGlobal(category, is_player=false) {
        const db = this.getDatabase(category),
            key_name = "global_last_crunch_participant_id" + (is_player?"_player":""),
            target = category + (is_player?"_player":"");
        // get lcpid from keys table
        let last_crunch_participant_id = await this.getKey(category, key_name, 0);

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
                await this.forward(this.getTarget(target), p.api_id,
                    { persistent: true }));

            // update lpcid & refetch
            if (participations.length > 0) {
                last_crunch_participant_id = participations[participations.length-1].id;
                await this.setKey(category, key_name,
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
}
