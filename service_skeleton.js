#!/usr/bin/env node
/* jshint esnext: true */
"use strict";

const Promise = require("bluebird");

const logger = global.logger,
    KEY_TYPE = process.env.KEY_TYPE || "crunch";  // backwards compat

module.exports = class Service {
    setRoutes(routes) {  // route str -> func
        this.routes = new Map(Object.entries(routes));
    }

    setDatabases(dbs, defaultDb) {  // mode str -> Sequelize
        this.dbs = new Map(Object.entries(dbs));
    }
    getDatabase(category) {
        if (!this.dbs.has(category))
            logger.error("unsupported database category");
        return this.dbs.get(category);
    }

    async register(rmqChannel, expressApp) {  // assert queues, initialize routes
        this.notify = async (topic, msg) =>
            await rmqChannel.publish("amq.topic", topic, new Buffer(msg));
        this.forward = async (queue, payload, options) =>
            await rmqChannel.sendToQueue(queue, new Buffer(payload), options);

        await Promise.all(this.targets, (queue) =>
            rmqChannel.assertQueue(queue, { durable: true }));

        for(let [route, func] of this.routes) {
            expressApp.post(route, func);
        }
    }

    setTargets(queues) {  // set channels messages can be forwarded to, str -> str
        this.targets = new Map(Object.entries(queues));
    }
    getTarget(category) {  // return channel for category
        if (!this.targets.has(category))
            logger.error("unsupported queue category", category);
        return this.targets.get(category);
    }

    // return an entry from keys db
    async getKey(category, key, default_value) {
        const db = this.getDatabase(category),
            record = await db.Keys.findOrCreate({
                where: {
                    type: KEY_TYPE,
                    key: key
                },
                defaults: { value: default_value }
            });
        return record[0].value;
    }

    // update an entry from keys db
    async setKey(category, key, value) {
        const db = this.getDatabase(category),
            record = await db.Keys.findOrCreate({
                where: {
                    type: KEY_TYPE,
                    key: key
                },
                defaults: { value: value }
            });
        record[0].update({ value: value });
        return value;
    }
}
