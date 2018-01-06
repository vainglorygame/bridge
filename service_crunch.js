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
    CRUNCH_HEROVSHERO_QUEUE = process.env.CRUNCH_HEROVSHERO_QUEUE || "crunch_herovshero",
    CRUNCH_TOURNAMENT_QUEUE = process.env.CRUNCH_TOURNAMENT_QUEUE || "crunch_global_tournament",
    CRUNCH_TOURNAMENT_BAN_QUEUE = process.env.CRUNCH_TOURNAMENT_BAN_QUEUE || "crunch_ban_tournament",
    CRUNCH_TOURNAMENT_PHASE_QUEUE = process.env.CRUNCH_TOURNAMENT_PHASE_QUEUE || "crunch_phase_tournament",
    CRUNCH_TOURNAMENT_PLAYER_QUEUE = process.env.CRUNCH_TOURNAMENT_PLAYER_QUEUE || "crunch_player_tournament",
    CRUNCH_TOURNAMENT_HEROVSHERO_QUEUE = process.env.CRUNCH_TOURNAMENT_HEROVSHERO_QUEUE || "crunch_tournament_herovshero",
    CRUNCH_BRAWL_QUEUE = process.env.CRUNCH_BRAWL_QUEUE || "crunch_global_brawl",
    CRUNCH_BRAWL_BAN_QUEUE = process.env.CRUNCH_BRAWL_BAN_QUEUE || "crunch_ban_brawl",
    CRUNCH_BRAWL_PHASE_QUEUE = process.env.CRUNCH_BRAWL_PHASE_QUEUE || "crunch_phase_brawl",
    CRUNCH_BRAWL_PLAYER_QUEUE = process.env.CRUNCH_BRAWL_PLAYER_QUEUE || "crunch_player_brawl",
    CRUNCH_BRAWL_HEROVSHERO_QUEUE = process.env.CRUNCH_BRAWL_HEROVSHERO_QUEUE || "crunch_brawl_herovshero",
    SHOVEL_SIZE = parseInt(process.env.SHOVEL_SIZE) || 1000;

module.exports = class Cruncher extends Service {
    constructor() {
        super();

        this.setTargets({
            "regular_player": CRUNCH_PLAYER_QUEUE,
            "regular+": CRUNCH_QUEUE,
            "regular+phase": CRUNCH_PHASE_QUEUE,
            "regular+ban": CRUNCH_BAN_QUEUE,
            "regular+herovshero": CRUNCH_HEROVSHERO_QUEUE,
            "tournament_player": CRUNCH_TOURNAMENT_PLAYER_QUEUE,
            "tournament+": CRUNCH_TOURNAMENT_QUEUE,
            "tournament+phase": CRUNCH_TOURNAMENT_PHASE_QUEUE,
            "tournament+ban": CRUNCH_TOURNAMENT_BAN_QUEUE,
            "tournament+herovshero": CRUNCH_TOURNAMENT_HEROVSHERO_QUEUE,
            "brawl_player": CRUNCH_BRAWL_PLAYER_QUEUE,
            "brawl+": CRUNCH_BRAWL_QUEUE,
            "brawl+phase": CRUNCH_BRAWL_PHASE_QUEUE,
            "brawl+ban": CRUNCH_BRAWL_BAN_QUEUE,
            "brawl+herovshero": CRUNCH_BRAWL_HEROVSHERO_QUEUE
        });

        this.setRoutes({
            // crunch global meta
            "/api/crunch/:category*?": async (req, res) => {
                this.crunchGlobal(req.params.category || "regular");
                res.sendStatus(204);
            },
            "/api/herovsherocrunch/:category*?": async (req, res) => {
                this.crunchGlobal(req.params.category || "regular", "herovshero");
                res.sendStatus(204);
            },
            // crunch global Telemetry meta
            "/api/phasecrunch/:category*?": async (req, res) => {
                this.crunchPhases(req.params.category || "regular", "phase");
                res.sendStatus(204);
            },
            "/api/bancrunch/:category*?": async (req, res) => {
                this.crunchPhases(req.params.category || "regular", "ban");
                res.sendStatus(204);
            },
            // crunch a player if they have not been crunched ever
            "/api/player/:name/crunch/:category*?": async (req, res) => {
                const category = req.params.category || "regular",
                    db = this.getDatabase(req.params.category || "regular"),
                    players = await db.Player.findAll({
                        where: { name: req.params.name }
                    });
                if (players == undefined) {
                    logger.error("player not found in db, won't crunch",
                        { name: req.params.name });
                    res.sendStatus(404);
                    return;
                }
                await Promise.all(players.map(async (player) => {
                    await this.crunchPlayer(category, player);
                }));
                // fire away
                res.sendStatus(204);
            }
        });
    }

    // crunch player's stats
    async crunchPlayer(category, player) {
        const db = this.getDatabase(category);

        const last_crunch_id = player.last_crunch_id || 0;

        // get all participants for this player since last crunch
        const participations = await db.Participant.findAll({
            attributes: [ "id", "api_id" ],
            where: {
                player_api_id: player.api_id,
                id: { $gt: last_crunch_id }
            },
            order: [ ["id", "ASC"] ]
        });

        logger.info("sending participations to player cruncher",
            { length: participations.length });

        if (participations.length > 0) {
            await player.update({ last_crunch_id: participations[participations.length-1 ].id });
        }

        await Promise.map(participations, async (p) => {
            await this.notify("player." + player.name, "crunch_pending");

            await this.forward(this.getTarget(category + "_player"),
                p.api_id, {
                    persistent: true,
                    headers: { notify: "player." + player.name }
                }
            );
        });
    }

    // crunch global stats
    async crunchGlobal(db_identifier, queue_identifier="") {
        const db = this.getDatabase(db_identifier),
            key_name = "global_last_crunch_participant_id" + queue_identifier;
        if (db == undefined) return;

        // get lcpid from keys table
        let last_crunch_participant_id = await this.getKey(db_identifier, key_name, 0);

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
                await this.forward(this.getTarget(`${db_identifier}+${queue_identifier}`), p.api_id,
                    { persistent: true }));

            // update lpcid & refetch
            if (participations.length > 0) {
                last_crunch_participant_id = participations[participations.length-1].id;
                await this.setKey(db_identifier, key_name,
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
    async crunchPhases(db_identifier, queue_identifier="") {
        const db = this.getDatabase(db_identifier),
            key_name = "global_last_crunch_participant_phase_id" + queue_identifier;
        // get lcphid from keys table
        let last_crunch_phase_id = await this.getKey(db_identifier, key_name, 0);

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
            await Promise.map(phases, async (ph) =>
                await this.forward(this.getTarget(`${db_identifier}+${queue_identifier}`), ph.id.toString(),
                    { persistent: true }));

            // update lphcid & refetch
            if (phases.length > 0) {
                last_crunch_phase_id = phases[phases.length-1].id;
                await this.setKey(db_identifier, key_name, last_crunch_phase_id);
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
