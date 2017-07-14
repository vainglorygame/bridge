#!/usr/bin/env node
/* jshint esnext: true */
"use strict";

const Promise = require("bluebird"),
    Service = require("./service_skeleton.js");

const logger = global.logger,
    CRUNCH_QUEUE = process.env.CRUNCH_QUEUE || "crunch",
    CRUNCH_TOURNAMENT_QUEUE = process.env.CRUNCH_TOURNAMENT_QUEUE || "crunch_tournament",
    SHOVEL_SIZE = parseInt(process.env.SHOVEL_SIZE) || 1000;

module.exports = class Cruncher extends Service {
    constructor() {
        super();

        this.setTargets({
            "regular": CRUNCH_QUEUE,
            "tournament": CRUNCH_TOURNAMENT_QUEUE
        });

        this.setRoutes({
            // crunch global meta
            "/api/crunch/:category*?": async (req, res) => {
                this.crunchGlobal(req.params.category || "regular");
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
            },
            // crunch a team
            "/api/team/:id/crunch": async (req, res) => {
                const db = this.getDatabase("regular"),
                    team = await db.Team.findOne({ where: { id: req.params.id } });
                if (team == undefined) {
                    logger.error("team not found in db, won't crunch",
                        { name: req.params.id });
                    res.sendStatus(404);
                    return;
                }
                logger.info("team in db, crunching", { name: team.id });
                crunchTeam(team.id);  // fire away
                res.sendStatus(204);
            }
        });
    }

    // upcrunch player's stats
    async crunchPlayer(category, api_id) {
        const db = this.getDatabase(category),
            where = { player_api_id: api_id },
            last_crunch = await db.PlayerPoint.findOne({
                attributes: ["updated_at"],
                where,
                order: [ ["updated_at", "DESC"] ]
            });
        if (last_crunch) where.created_at = { $gt: last_crunch.updated_at };

        // get all participants for this player
        const participations = await db.Participant.findAll({
            attributes: ["api_id"],
            where
        });
        // send everything to cruncher
        logger.info("sending participations to cruncher",
            { length: participations.length });
        await Promise.map(participations, async (p) =>
            await this.forward(this.getQueue(category),
                p.api_id, { persistent: true, type: "player" }));
        // jobs with the type "player" won't be taken into account for global stats
        // global stats would increase on every player refresh otherwise
    }

    // reset fame and crunch
    // TODO: incremental crunch possible?
    async crunchTeam(team_id) {
        await this.forward(CRUNCH_QUEUE, team_id,
            { persistent: true, type: "team" });
    }

    // crunch global stats
    async crunchGlobal(category) {
        const db = this.getDatabase(category);
        // get lcpid from keys table
        let last_crunch_participant_id = await this.getKey(db,
            "global_last_crunch_participant_id", 0);

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
                await this.forward(this.getQueue(category), p.api_id,
                    { persistent: true, type: "global" }));

            // update lpcid & refetch
            if (participations.length > 0) {
                last_crunch_participant_id = participations[participations.length-1].id;
                await this.setKey(db, "global_last_crunch_participant_id",
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
