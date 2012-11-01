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
    numcol = parseInt(process.env.numcol, 10),
    popclients = [],
    pushclients = [];
for (var i = 0; i < numcol; i += 1) {
	popclients.push(redis.createClient(parseInt(process.env.redisport, 10), process.env.redishost, { parser: process.env.redisparser }));
	pushclients.push(redis.createClient(parseInt(process.env.redisport, 10), process.env.redishost, { parser: process.env.redisparser }));
}

var Worker = {
	constants: require('./Constants.js'),
	_id: parseInt(process.env.id, 10),
	_seperator: process.env.seperator,
	_taskchannel: process.env.taskchannel,
	_resultchannel: process.env.resultchannel,
	_TaskHandlers: {},
	_poll: function(num) {
		var _this = this
		//console.log("POLL:",this._taskchannel+num.toString());
		popclients[num].blpop(this._taskchannel+num.toString(), 0, function(err, str) {
			//console.log("FOUND:", _this._id, err, str);
			if (!err && str) {
				var sep = _this._seperator,
					data = str[1].split(sep),
					taskId = data.shift(),
					event = data.shift(),
					__this = _this;
				//console.log("HANDLER:", event, data);
				data.push(function() {
					//console.log("RCALL:", arguments);
					var value = taskId,
						n = num;
					for (var i = 0, l = arguments.length; i < l; i += 1) {
						value += sep+arguments[i];
					}
					pushclients[n].rpush(__this._resultchannel+n.toString(), value, function() {
						//console.log("ANSWER:", __this._resultchannel+n.toString(), value);
						__this._poll(n);
					});
				});
				__this._TaskHandlers[event].apply(this, data);
			} else {
				console.log("ERROR:", err);
				_this._poll(num);
			}
		});
	},
	on: function(event, func) {
		this._TaskHandlers[event] = func;
	}
};
for (var i = 0, l = numcol; i < l; i += 1) {
	Worker._poll(i);
}

/*var _TaskHandlers = {};
Worker.onTask = function(event, func) {
	_TaskHandlers[event] = func;
};

process.send({ type: Worker.constants.MSG_GET_WORK });

process.on('message', function(data) {
	try {
		_TaskHandlers[data.data.e](data.data.d, function(err, result) {
			data.err  = err;
			data.data.d = result;
			data.type = Worker.constants.MSG_SEND_RESULT;
			process.send(data);
		});
	} catch (e) {
		console.log("Error in Worker "+Worker.id+": "+e.message);
		data.err = true;
		delete data.data.d;
		data.type = Worker.constants.MSG_SEND_RESULT;
		process.send(data);
	}
});*/

module.exports = Worker;