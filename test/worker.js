module.exports = function(worker) {
	var counter = 0,
		start;
	worker.on('test', function(data, callback) {
		if (counter === 0) start = new Date().getTime();
		//console.log("WORK:", data);
		callback(null, parseInt(data, 10)+1);
		if (counter++ === 10000) console.log((new Date().getTime())-start);
		if (counter === 15000) console.log((new Date().getTime())-start);
	});
};