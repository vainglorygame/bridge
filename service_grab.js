#!/usr/bin/env node
/* jshint esnext: true */
"use strict";

const api = require("../orm/api"),
    Promise = require("bluebird"),
    Seq = require("sequelize"),
    Service = require("./service_skeleton.js");

const GRABSTART = process.env.GRABSTART || "2017-02-14T00:00:00Z",
    BRAWL_RETENTION_DAYS = parseInt(process.env.BRAWL_RETENTION_DAYS) || 3,
    PLAYER_PROCESS_QUEUE = process.env.PLAYER_PROCESS_QUEUE || "process",
    PLAYER_TOURNAMENT_PROCESS_QUEUE = process.env.PLAYER_TOURNAMENT_PROCESS_QUEUE || "process_tournament",
    GRAB_QUEUE = process.env.GRAB_QUEUE || "grab",
    GRAB_BRAWL_QUEUE = process.env.GRAB_BRAWL_QUEUE || "grab_brawl",
    GRAB_TOURNAMENT_QUEUE = process.env.GRAB_TOURNAMENT_QUEUE || "grab_tournament",
    REGIONS = (process.env.REGIONS || "na,eu,sg,sa,ea").split(","),
    TOURNAMENT_REGIONS = (process.env.TOURNAMENT_REGIONS || "tournament-na," +
        "tournament-eu,tournament-sg,tournament-sa,tournament-ea").split(",");

module.exports = class Analyzer extends Service {
    constructor() {
        super();

        this.setTargets({
            "regular": GRAB_QUEUE,
            "brawl": GRAB_BRAWL_QUEUE,
            "tournament": GRAB_TOURNAMENT_QUEUE,
            "regular_player": PLAYER_PROCESS_QUEUE,
            "tournament_player": PLAYER_TOURNAMENT_PROCESS_QUEUE
        });

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
                    players = await db.Player.findAll({ where: { name: req.params.name } });
                if (players == undefined) {
                    logger.error("player not found in db", { name: req.params.name });
                    res.sendStatus(404);
                    return;
                }
                logger.info("player in db, updating", { name: name, category: category });
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
            case "regular": return "casual,ranked";
            case "brawl": return "casual_aral,blitz_pvp_ranked";
            case "tournament": return "private,private_party_draft_match";
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
            case "brawl": 
                let past = new Date();
                past.setDate(past.getDate() - BRAWL_RETENTION_DAYS);
                return past.toISOString();  // minimum
            case "tournament": return "2017-02-12T00:00:00Z";  // all
        }
        logger.error("unsupported grabstart category", { category: category });
    }

    // API requires maximum time interval of 4w
    // return [payload, payload, …] with fixed createdAt
    getSplitGrabs(payload, start, end) {
        let part_start = start, part_end = start,
            payloads = [];
        // loop forwards, adding 4w until we passed NOW
        while (part_start < end) {
            part_end = new Date(part_end.getTime() + (4 * 60*60*24*7*1000));  // add 4w
            if(part_end > end) part_end = end;
            let pl = payload;
            pl["params"]["filter[createdAt-start]"] = part_start.toISOString();
            pl["params"]["filter[createdAt-end]"] = part_end.toISOString();
            // push a deep clone or the object will have the reference to part_start
            // which leads to all start/end dates being the same (?!)
            payloads.push(JSON.parse(JSON.stringify(pl)));  // JavaScript sucks.
            part_start = part_end;
        }
        return payloads;
    }

    // request grab jobs for a player's matches
    async grabPlayer(name, region, last_match_created_date, id, category) {
        const start = new Date(Date.parse(last_match_created_date)),
            end = new Date();  // today

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

            await this.forward(this.getTarget(category), JSON.stringify(payload), {
                persistent: true,
                headers: { notify: "player." + name }
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
        logger.info("requesting triple player grab", {
            names: names, region: region, category: category
        });

        await Promise.each(this.getSplitGrabs(payload_template, grabstart, now), async (payload) => {
            await this.forward(this.getTarget(category), JSON.stringify(payload), {
                persistent: true,
                headers: { notify: "player." + names }
            });
        });
    }

    // request grab jobs for a region's matches
    async grabMatches(region, lmcd) {
        if (lmcd == undefined) lmcd = new Date(Date.parse(this.getGrabstart("tournament")));
        const now = new Date();

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
            await this.forward(this.getTarget(category + "_player"), JSON.stringify(player), {
                persistent: true, type: "player",
                headers: { notify: "player." + player.name }
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
        if (player.get("last_update") != null && category == "regular")
            // TODO do not fetch everything on category tournament / brawl
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
