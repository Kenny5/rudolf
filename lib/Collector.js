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

var redis = require('redis'),
    popclient = redis.createClient(parseInt(process.env.redisport, 10), process.env.redishost, { parser: process.env.redisparser }),
    pushclient = redis.createClient(parseInt(process.env.redisport, 10), process.env.redishost, { parser: process.env.redisparser });

var Collector = {
	constants: require('./Constants.js'),
	debug: process.env.debug,
	_seperator: process.env.seperator,
	_id: parseInt(process.env.id),
	_max: parseInt(process.env.max),
	_taskchannel: process.env.taskchannel,
	_resultchannel: process.env.resultchannel,
	_getTaskId: function() {
		var end = this._max,
			counter = 0;
		var __getTaskId = function() {
			if (counter < end) counter += 1;
			else counter = 0;
			return counter;
		};
		this._getTaskId = __getTaskId;
		return this._getTaskId();	
	},
	_HandleStack: {},
	_task: { id: null,
			 value: '' },
	_poll: function() {
		var _this = this;
		//console.log("PREGOT:", this._resultchannel);
		popclient.blpop(this._resultchannel, 0, function(err, result) {
			//console.log("GOT:", err, result)
			if (!err && result) {

					var data = result[1].split(_this._seperator),
						taskId = parseInt(data.shift(), 10);
				try {
					_this._HandleStack[taskId].apply(this, data);
					delete _this._HandleStack[taskId];
				} catch (e) {
					console.log("ERROR:", e.message, data, taskId);
				}
			} else {
				console.log('ERROR:', err);
			}
			_this._poll();
		});
	},
	task: function(event) {
		this._task.id = this._getTaskId();
		this._task.value = this._task.id.toString()+this._seperator+event;
		return this;
	},
	args: function() {
		for (var i = 0, l = arguments.length; i < l; i += 1) {
			this._task.value += this._seperator+arguments[i];
		}
		return this;
	},
	exec: function(callback) {
		this._HandleStack[this._task.id] = callback;
		//console.log("ADDED:", this._taskchannel, this._task.value);
		pushclient.rpush(this._taskchannel, this._task.value, function(err) {
			if (err) console.log('ERROR:', err);
		});
	}
};
Collector._taskchannel += Collector._id.toString();
Collector._resultchannel += Collector._id.toString();
Collector._poll();


module.exports = Collector