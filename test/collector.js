module.exports = function(collector) {
	
	var start = new Date().getTime();

	var test_length = 20000;
	var test_counter = 0;
	var test = function(a, b) {
		if (a === (b-1)) test_counter++;
		if (test_counter === test_length) { 
			var end = new Date().getTime(),
				time = end - start;
			console.log("Collector:", collector._id, "got all "+test_length+" results back in", time+"ms");
		}
	};

	var i = test_length;
	while(i--) {
		(function(i) {
			collector.task('test').args(i).exec(function(err, result) {
				test(i, parseInt(result, 10));
			});		
		})(i);
	}

	/*var x = 10, y = 20;
	collector.task('test').args(x).exec(function(err, result) {
		console.log(result);
	});*/
};