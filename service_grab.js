#!/usr/bin/env node
/* jshint esnext: true */
"use strict";

const api = require("../orm/api"),
    Promise = require("bluebird"),
    Seq = require("sequelize"),
    Service = require("./service_skeleton.js");

const GRABSTART = process.env.GRABSTART || "2017-02-01T00:00:00Z",
    BRAWL_GRABSTART = process.env.BRAWL_GRABSTART || "2017-02-01T00:00:00Z",
    // pipe inputs (special to service_grab, see below)
    PLAYER_PROCESS_QUEUE = process.env.PLAYER_PROCESS_QUEUE || "process",
    PLAYER_BRAWL_PROCESS_QUEUE = process.env.PLAYER_BRAWL_PROCESS_QUEUE || "process_brawl",
    PLAYER_TOURNAMENT_PROCESS_QUEUE = process.env.PLAYER_TOURNAMENT_PROCESS_QUEUE || "process_tournament",
    GRAB_QUEUE = process.env.GRAB_QUEUE || "grab",
    GRAB_BRAWL_QUEUE = process.env.GRAB_BRAWL_QUEUE || "grab_brawl",
    GRAB_TOURNAMENT_QUEUE = process.env.GRAB_TOURNAMENT_QUEUE || "grab_tournament",
    // pipe outputs allow an object destined for one category to reach multiple targets
    PIPE_REGULAR_OUT = (process.env.PIPE_REGULAR_OUT || "regular").split(","),
    PIPE_BRAWL_OUT = (process.env.PIPE_BRAWL_OUT || "brawl").split(","),
    PIPE_TOURNAMENT_OUT = (process.env.PIPE_TOURNAMENT_OUT || "tournament").split(","),
    PIPE_REGULAR_PLAYER_OUT = (process.env.PIPE_REGULAR_PLAYER_OUT || "regular_player,brawl_player").split(","),
    PIPE_BRAWL_PLAYER_OUT = (process.env.PIPE_BRAWL_PLAYER_OUT || "regular_player,brawl_player").split(","),
    PIPE_TOURNAMENT_PLAYER_OUT = (process.env.PIPE_TOURNAMENT_PLAYER_OUT || "tournament_player").split(","),
    // dynamic region setup
    REGIONS = (process.env.REGIONS || "na,eu,sg,sa,ea,cn").split(","),
    TOURNAMENT_REGIONS = (process.env.TOURNAMENT_REGIONS || "tournament-na," +
        "tournament-eu,tournament-sg,tournament-sa,tournament-ea").split(","),
    REGULAR_MODES = process.env.REGULAR_MODES || "casual,ranked",
    BRAWL_MODES = process.env.BRAWL_MODES || "casual_aral,blitz_pvp_ranked",
    TOURNAMENT_MODES = process.env.TOURNAMENT_MODES || "private,private_party_draft_match";

module.exports = class Analyzer extends Service {
    constructor() {
        super();

        // TODO not all queues are being created
        this.setTargets({
            "regular": GRAB_QUEUE,
            "brawl": GRAB_BRAWL_QUEUE,
            "tournament": GRAB_TOURNAMENT_QUEUE,
            "regular_player": PLAYER_PROCESS_QUEUE,
            "brawl_player": PLAYER_BRAWL_PROCESS_QUEUE,
            "tournament_player": PLAYER_TOURNAMENT_PROCESS_QUEUE
        });

        // category=[pipe in] spreads to multiple categories=[pipe out]
        // Map { pipe in: [pipe out 1, pipe out 2, …] }
        this.categoryPipe = new Map(Object.entries({
            "regular": PIPE_REGULAR_OUT,
            "brawl": PIPE_BRAWL_OUT,
            "tournament": PIPE_TOURNAMENT_OUT,
            "regular_player": PIPE_REGULAR_PLAYER_OUT,
            "brawl_player": PIPE_BRAWL_PLAYER_OUT,
            "tournament_player": PIPE_TOURNAMENT_PLAYER_OUT
        }));

        // wrapper around getTarget to support pipes
        this.getTargets = (pipeIn) =>
            this.categoryPipe.get(pipeIn)
            .map((pipeOut) =>
                this.getTarget(pipeOut));

        // setup API endpoints
        this.setRoutes({
            // update a whole region (tournament only)
            "/api/match/:region/update": async (req, res) => {
                const region = req.params.region;
                if (TOURNAMENT_REGIONS.indexOf(region) == -1) {
                    logger.error("called with unsupported region", { region: region });
                    res.sendStatus(404);
                    return;
                }
                this.updateRegion(region, "tournament");  // fire away
                res.sendStatus(204);
            },
            // grab multiple users by IGN
            "/api/players/:region/grab/:category*?": async (req, res) => {
                let grabstart = new Date();
                grabstart.setTime(grabstart.getTime() - req.query.minutesAgo * 60000);
                this.grabPlayers(req.query.names, req.params.region, grabstart,
                    req.params.category || "regular");
                res.sendStatus(204);
            },

            // find a player / force a player update
            "/api/player/:name/search/:category*?": async (req, res) => {
                this.searchPlayer(req.params.name, req.params.category || "regular");
                res.sendStatus(204);  // notifications will follow
            },
            // update a known user
            // flow:
            //   * get from db (depending on category, either regular, brawl or tourn)
            //   * update lifetime from `/players` via id, catch IGN changes
            //   * update from `/matches`
            "/api/player/:name/update/:category*?": async (req, res) => {
                const name = req.params.name,
                    category = req.params.category || "regular",
                    db = this.getDatabase(category),
                    players = await db.Player.findAll({ where: { name } });
                if (players == undefined) {
                    logger.error("player not found in db", { name });
                    res.sendStatus(404);
                    return;
                }
                logger.info("player in db, updating", { name: name, category: category });
                players.forEach((player) => this.updatePlayer(player, category));
                res.sendStatus(204);
            },
            "/api/player/api_id/:api_id/update/:category*?": async (req, res) => {
                const api_id = req.params.api_id,
                    category = req.params.category || "regular",
                    db = this.getDatabase(category),
                    players = await db.Player.findAll({ where: { api_id } });
                if (players == undefined) {
                    logger.error("player not found in db", { api_id });
                    res.sendStatus(404);
                    return;
                }
                logger.info("player in db, updating", { api_id, category });
                players.forEach((player) => this.updatePlayer(player, category));
                res.sendStatus(204);
            },
            // update a random user
            "/api/player": async (req, res) => {
                // TODO this becomes slow
                let player = await this.getDatabase("regular").Player.findOne({ order: [ Seq.fn("RAND") ] });
                this.updatePlayer(player, "regular");
                res.json(player);
            }
        });
    }

    // convenience
    getGameModes(category) {
        switch (category) {
            case "regular": return REGULAR_MODES;
            case "brawl": return BRAWL_MODES;
            case "tournament": return TOURNAMENT_MODES;
        }
        logger.error("unsupported game mode category", { category: category });
    }
    getRegions(category) {
        switch (category) {
            case "brawl":
            case "regular": return REGIONS;
            case "tournament": return TOURNAMENT_REGIONS;
        }
        logger.error("unsupported region category", { category: category });
    }
    // return a default `createdAt-start` based on game mode
    getGrabstart(category) {
        switch (category) {
            case "regular": return GRABSTART;
            case "brawl": return BRAWL_GRABSTART;
            case "tournament": return "2017-02-12T00:00:00Z";  // all
        }
        logger.error("unsupported grabstart category", { category: category });
    }

    // API requires maximum time interval of 4w
    // return [payload, payload, …] with fixed createdAt
    getSplitGrabs(payload, start, end) {
        let last_start = new Date(start.getTime()),
            payloads = [];
        // loop forwards, adding 1 month until we passed NOW
        while (last_start < end) {
            /*
            let this_end = new Date(last_start.getTime() - 1000);
            this_end.setMonth(this_end.getMonth() + 1);
            */
            let this_end = new Date(last_start.getTime() + (4 * 60*60*24*7*1000));
            let this_start = new Date(last_start.getTime());
            // query from `start` to `start + 1 month - 1s`, hitting the full month

            if(this_end > end) this_end = end;
            let pl = JSON.parse(JSON.stringify(payload));
            pl["params"]["filter[createdAt-start]"] = this_start.toISOString();
            pl["params"]["filter[createdAt-end]"] = this_end.toISOString();
            payloads.push(pl);
            // add +1s so we do not overlap start&end and are in the next month
            last_start = new Date(this_end.getTime() + 1000);
        }
        return payloads;
    }

    // request grab jobs for a player's matches
    async grabPlayer(name, region, last_match_created_date, id, category) {
        const start = new Date(Date.parse(last_match_created_date)),
            end = new Date();  // today
        end.setMinutes(end.getMinutes() - 1);  // TODO API hotfix, server time sync issue

        const payload_template = {
            "region": region,
            "params": {
                "filter[playerIds]": id,
                "filter[gameMode]": this.getGameModes(category),
                "sort": "createdAt"
            }
        };
        await Promise.each(this.getSplitGrabs(payload_template, start, end), async (payload) => {
            logger.info("requesting update", { name: name, region: region });

            await Promise.map(this.getTargets(category), async (target) => {
                await this.forward(target, JSON.stringify(payload), {
                    persistent: true,
                    headers: { notify: "player." + name }
                });
            });
        });
    }

    // request grab job for multiple players' matches
    async grabPlayers(names, region, grabstart, category) {
        const payload_template = {
            "region": region,
            "params": {
                "filter[playerNames]": names,
                "filter[gameMode]": this.getGameModes(category),
                "sort": "createdAt"
            }
        }, now = new Date();
        now.setMinutes(now.getMinutes() - 1);  // TODO API hotfix, server time sync issue
        logger.info("requesting triple player grab", {
            names: names, region: region, category: category
        });

        await Promise.each(this.getSplitGrabs(payload_template, grabstart, now), async (payload) => {
            await Promise.map(this.getTargets(category), async (target) => {
                await this.forward(target, JSON.stringify(payload), {
                    persistent: true,
                    headers: { notify: "player." + names }
                });
            });
        });
    }

    // request grab jobs for a region's matches
    async grabMatches(region, lmcd) {
        if (lmcd == undefined) lmcd = new Date(Date.parse(this.getGrabstart("tournament")));
        const now = new Date();
        now.setMinutes(now.getMinutes() - 1);  // TODO API hotfix, server time sync issue

        // add 1s, because createdAt-start <= x <= createdAt-end
        // so without the +1s, we'd always get the last_match_created_date match back
        lmcd.setSeconds(lmcd.getSeconds() + 1);

        const payload_template = {
            "region": region,
            "params": { "sort": "createdAt" }
        };
        logger.info("requesting region update", { region: region });

        await Promise.each(this.getSplitGrabs(payload_template, lmcd, now), async (payload) => {
            await this.forward(GRAB_TOURNAMENT_QUEUE,
                JSON.stringify(payload), { persistent: true });
        });
    }

    // search for a player name in one region
    // request process for lifetime
    // return an array (JSONAPI response)
    async searchPlayerInRegion(region, name, id, category) {
        logger.info("searching", { name: name, id: id, region: region });
        let options = {};
        if (id == undefined) options["filter[playerNames]"] = name
        else options["filter[playerIds]"] = id

        // players.length and page length will be 1 in 99.9999% of all cases
        // - but just in case.
        const players = await Promise.map(await api.requests("players", region, options, logger), async (player) => {
            logger.info("found", { name: name, id: id, region: region });
            await this.notify("player." + name, "search_success");
            // send to processor, so the player is in db
            // no matter whether we find matches or not
            await Promise.map(this.getTargets(category + "_player"), async (target) => {
                await this.notify("player." + player.name, "player_pending");
                await this.forward(target, JSON.stringify(player), {
                    persistent: true, type: "player",
                    headers: { notify: "player." + player.name }
                });
            });
            return player;
        });
        if (players.length == 0)
            logger.warn("not found", { name: name, id: id, region: region });

        return players;
    }

    // search for player name on all shards
    // grab player(s)
    // send a notification for results and request updates
    async searchPlayer(name, category) {
        let found = false;
        logger.info("searching", { name: name });
        await Promise.map(this.getRegions(category), async (region) => {
            const players = await this.searchPlayerInRegion(region, name, undefined, category);
            if (players.length > 0) found = true;

            // request grab jobs
            await Promise.map(players, async (p) =>
                await this.grabPlayer(p.name, p.shardId,
                    this.getGrabstart(category), p.id, category));
        });
        // notify web
        if (!found) {
            logger.info("search failed", { name: name });
            await this.notify("player." + name, "search_fail");
        }
    }

    // update a player from API based on db record
    // & grab player
    async updatePlayer(player, category) {
        // set last_update and request an update job
        // if last_update is null, we need that player's full history
        let grabstart;
        if (player.get("last_update") != null)
            grabstart = player.last_match_created_date;
        if (grabstart == undefined) grabstart = this.getGrabstart(category);
        else
            // add 1s, because createdAt-start <= x <= createdAt-end
            // so without the +1s, we'd always get the last_match_created_date match back
            grabstart.setSeconds(grabstart.getSeconds() + 1);

        const players = await this.searchPlayerInRegion(
            player.shard_id, player.name, player.api_id, category);

        // will be the same as `player` 99.9% of the time
        // but not when the player changed their name
        // search happened by ID, so we get only 1 player back
        // but player.name != players[0].attributes.name
        // (with a name change)
        await Promise.map(players, async (p) =>
            await this.grabPlayer(p.name, p.shardId, grabstart, p.id, category));
    }

    // update a region from API based on db records
    async updateRegion(region, category) {
        let grabstart;
        const db = this.getDatabase(category),
            last_match = await db.Participant.findOne({
                attributes: ["created_at"],
                where: { shard_id: region },
                order: [ [Seq.col("created_at"), "DESC"] ]
            });
        if (last_match == null) grabstart = undefined;
        else grabstart = last_match.get("created_at");

        await this.grabMatches(region, grabstart);
    }

}
