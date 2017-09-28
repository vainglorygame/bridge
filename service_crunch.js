#!/usr/bin/env node
/* jshint esnext: true */
"use strict";

const Promise = require("bluebird"),
    Service = require("./service_skeleton.js");

const logger = global.logger,
    CRUNCH_QUEUE = process.env.CRUNCH_QUEUE || "crunch_global",
    CRUNCH_BAN_QUEUE = process.env.CRUNCH_BAN_QUEUE || "crunch_ban",
    CRUNCH_PHASE_QUEUE = process.env.CRUNCH_PHASE_QUEUE || "crunch_phase",
    CRUNCH_PLAYER_QUEUE = process.env.CRUNCH_PLAYER_QUEUE || "crunch_player",
    CRUNCH_TOURNAMENT_QUEUE = process.env.CRUNCH_TOURNAMENT_QUEUE || "crunch_global_tournament",
    CRUNCH_TOURNAMENT_BAN_QUEUE = process.env.CRUNCH_TOURNAMENT_BAN_QUEUE || "crunch_ban_tournament",
    CRUNCH_TOURNAMENT_PHASE_QUEUE = process.env.CRUNCH_TOURNAMENT_PHASE_QUEUE || "crunch_phase_tournament",
    CRUNCH_TOURNAMENT_PLAYER_QUEUE = process.env.CRUNCH_TOURNAMENT_PLAYER_QUEUE || "crunch_player_tournament",
    CRUNCH_BRAWL_QUEUE = process.env.CRUNCH_BRAWL_QUEUE || "crunch_global_brawl",
    CRUNCH_BRAWL_BAN_QUEUE = process.env.CRUNCH_BRAWL_BAN_QUEUE || "crunch_ban_brawl",
    CRUNCH_BRAWL_PHASE_QUEUE = process.env.CRUNCH_BRAWL_PHASE_QUEUE || "crunch_phase_brawl",
    CRUNCH_BRAWL_PLAYER_QUEUE = process.env.CRUNCH_BRAWL_PLAYER_QUEUE || "crunch_player_brawl",
    SHOVEL_SIZE = parseInt(process.env.SHOVEL_SIZE) || 1000;

module.exports = class Cruncher extends Service {
    constructor() {
        super();

        this.setTargets({
            "regular": CRUNCH_QUEUE,
            "regular_player": CRUNCH_PLAYER_QUEUE,
            "regular_phase": CRUNCH_PHASE_QUEUE,
            "regular_ban": CRUNCH_BAN_QUEUE,
            "tournament": CRUNCH_TOURNAMENT_QUEUE,
            "tournament_player": CRUNCH_TOURNAMENT_PLAYER_QUEUE,
            "tournament_phase": CRUNCH_TOURNAMENT_PHASE_QUEUE,
            "tournament_ban": CRUNCH_TOURNAMENT_BAN_QUEUE,
            "brawl": CRUNCH_BRAWL_QUEUE,
            "brawl_player": CRUNCH_BRAWL_PLAYER_QUEUE,
            "brawl_phase": CRUNCH_BRAWL_PHASE_QUEUE,
            "brawl_ban": CRUNCH_BRAWL_BAN_QUEUE
        });

        this.setRoutes({
            // crunch global meta
            "/api/crunch/:category*?": async (req, res) => {
                this.crunchGlobal(req.params.category || "regular");
                res.sendStatus(204);
            },
            // crunch global Telemetry meta
            "/api/phasecrunch/:category*?": async (req, res) => {
                this.crunchPhases(req.params.category || "regular");
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
                    this.crunchPlayer(category, player.api_id, player.name));
                // fire away
                res.sendStatus(204);
            }
        });
    }

    // upcrunch player's stats
    async crunchPlayer(category, api_id, name) {
        const db = this.getDatabase(category),
            where = { player_api_id: api_id },
            last_crunch_r = await db.PlayerPoint.findOne({
                attributes: [ "updated_at" ],  // stores max created_at
                where,
                // TODO use dimensions "all" instead
                order: [ ["updated_at", "DESC"] ]
            });
        if (last_crunch_r) where.created_at = { $gt: last_crunch_r.updated_at };

        // get all participants for this player
        const participations = await db.Participant.findAll({
            attributes: [ "api_id", "created_at" ],
            where,
            order: [ ["created_at", "ASC" ] ]
        });
        // send everything to cruncher
        logger.info("sending participations to cruncher",
            { length: participations.length });
        await Promise.map(participations, async (p) => {
            await this.notify("player." + name, "crunch_pending");

            await this.forward(this.getTarget(category + "_player"),
                p.api_id, {
                    persistent: true,
                    headers: { notify: "player." + name }
                }
            );
        });
    }

    // crunch global stats
    async crunchGlobal(category) {
        const db = this.getDatabase(category),
            key_name = "global_last_crunch_participant_id";
        // get lcpid from keys table
        let last_crunch_participant_id = await this.getKey(category, key_name, 0);

        // don't load the whole Participant table at once into memory
        let participations;

        logger.info("loading all participations into cruncher",
            { last_crunch_participant_id });
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
                await this.forward(this.getTarget(category), p.api_id,
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
                last_crunch_participant_id
            });
        } while (participations.length == SHOVEL_SIZE);
        logger.info("done loading participations into cruncher");
    }

    // crunch global ban & phase stats
    async crunchPhases(category) {
        const db = this.getDatabase(category),
            key_name = "global_last_crunch_participant_phase_id";
        // get lcphid from keys table
        let last_crunch_phase_id = await this.getKey(category, key_name, 0);

        // don't load the whole Participant_phases table at once into memory
        let phases;

        logger.info("loading all phases into cruncher",
            { last_crunch_phase_id });
        do {
            phases = await db.ParticipantPhases.findAll({
                attributes: ["id"],
                where: {
                    id: { $gt: last_crunch_phase_id }
                },
                limit: SHOVEL_SIZE,
                order: [ ["id", "ASC"] ]
            });
            // tables are split, and so are services and queues
            await Promise.map(phases, async (ph) =>
                await this.forward(this.getTarget(category + "_phase"), ph.id.toString(),
                    { persistent: true }));
            await Promise.map(phases, async (ph) =>
                await this.forward(this.getTarget(category + "_ban"), ph.id.toString(),
                    { persistent: true }));

            // update lphcid & refetch
            if (phases.length > 0) {
                last_crunch_phase_id = phases[phases.length-1].id;
                await this.setKey(category, key_name, last_crunch_phase_id);
            }
            logger.info("loading more phases into cruncher", {
                limit: SHOVEL_SIZE,
                size: phases.length,
                last_crunch_phase_id
            });
        } while (phases.length == SHOVEL_SIZE);
        logger.info("done loading phases into cruncher");
    }
}
