var rudolf = require('../lib/rudolf.js');

rudolf.configure(function() {
	console.log("Configure multi-process test and start it!");

	rudolf.set({
		worker:       '../test/worker.js',
		collector: '../test/collector.js',
		debug: 						 true, 
	});
	rudolf.start({
		worker: 2,
		collector: 2,
	});
});
