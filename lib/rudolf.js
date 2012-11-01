/*
	taskprocess is Licensed using the revised BSD license.
	If you have any questions, contact Niklas Voss at <niklas.voss@gmail.com>
	
	
	 Copyright (c) 2012, the taskprocess developers (see AUTHORS file for credit).
	 All rights reserved.
	
	 Redistribution and use in source and binary forms, with or without
	 modification, are permitted provided that the following conditions are
	 met:
	 * Redistributions of source code must retain the above copyright
	   notice, this list of conditions and the following disclaimer.
	 * Redistributions in binary form must reproduce the above copyright
	   notice, this list of conditions and the following disclaimer in
	   the documentation and/or other materials provided with the
	   distribution.
	 * Neither the name of taskprocess nor the names of its contributors may
	   be used to endorse or promote products derived from this software
	   without specific prior written permission.
	
	THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
	IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
	TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
	PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER
	OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
	EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
	PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
	PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
	LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
	NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
	SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

'use strict';

var cluster = require('cluster'),
	redis = require("redis");

var rudolf = {
	constants: require('./Constants.js'),
	configure: function() {}
};

if (cluster.isMaster) {
	var collector_path = undefined,
		worker_path = undefined,
		Collectors = [],
		Workers = [],
		debug = false,
		seperator = '!',
		taskchannel = 'task',
		resultchannel = 'result',
		max_tasks = 100000,
		redis_conf = { port: 6379, host: '127.0.0.1', parser: 'javascript' };

	rudolf.configure = function(func) {
		func();
	};

	rudolf.set = function(config) {
		if (config.collector) collector_path = config.collector;
		if (config.worker) worker_path = config.worker;
		if (config.debug == true || config.debug == false) debug = config.debug;
		if (config.seperator) seperator = config.seperator;
		if (config.taskchannel) taskchannel = config.taskchannel;
		if (config.resultchannel) resultchannel = config.resultchannel;
		if (config.max) max_tasks = config.max;
		if (config.redis) {
			if (config.redis.host) redis_conf.host = config.redis.host;
			if (config.redis.port) redis_conf.port = config.redis.port;
			if (config.redis.parser) redis_conf.parser = config.redis.parser;
		}
		return this;
	};

	rudolf._clean = function(num) {
		console.log(redis);
		var client = redis.createClient(redis_conf.port, redis_conf.host, { parser: redis_conf.parser });
		console.log("Cleaning redis task queues...");
		while(num--) {
			(function(num) { // clean task channel
				client.del(taskchannel+num, function(err, msg) {
					if (err) console.log("ERROR:", err);
				});
			})(num);
			(function(num) { // clean result channel
				client.del(resultchannel+num, function(err, msg) {
					if (err) console.log("ERROR:", err);
				});
			})(num);
		}

	};

	rudolf.start = function(options) {
		var i = options.worker = options.worker || 1,
		    j = options.collector = options.collector || 1;
		if (worker_path && collector_path) {
			this._clean(i);
			if (!debug) cluster.setupMaster({ silent : true });
			console.log("Starting Child-Processes... W:", i, "C:", j);
			while(i--) Workers.push(cluster.fork({  type: this.constants.WORKER, 
													id: Workers.length, 
													numcol: options.collector, 
													script: worker_path, 
													seperator: seperator, 
													taskchannel: taskchannel, 
													resultchannel: resultchannel, 
													redisport: redis_conf.port,
													redishost: redis_conf.host,
													redisparser: redis_conf.parser,
													debug: debug }));
			while(j--) Collectors.push(cluster.fork({   type: this.constants.COLLECTOR, 
														id: Collectors.length, 
														max: max_tasks, 
														script: collector_path, 
														seperator: seperator, 
														taskchannel: taskchannel, 
														resultchannel: resultchannel, 
														redisport: redis_conf.port,
														redishost: redis_conf.host,
														redisparser: redis_conf.parser,
														debug: debug }));
		} else {
			throw "The worker and collector scripts are not defined!";
		}
		return this;
	};

	cluster.on('exit', function(worker, code, signal) {
    	console.log('Child-Process '+worker.process.pid+' died.');
  	});
    
} else if (cluster.isWorker) {

	if (process.env.type == rudolf.constants.COLLECTOR) {
		rudolf.Collector = require('./Collector.js');
		require(process.env.script)(rudolf.Collector);
	} else if (process.env.type == rudolf.constants.WORKER) {
		rudolf.Worker = require('./Worker.js');
		require(process.env.script)(rudolf.Worker);
	};

}

module.exports = rudolf;