var myChart = {};
window.onload = function() {
	var ctx = document.getElementById("myChart").getContext('2d');
	myChart = new Chart(ctx, {
		type: 'line'
	});
}


function updateGraph(event) {
	event.preventDefault();
	var formData = new FormData(document.querySelector('form'));
	var ipAddresses = formData.getAll("ipAddress");
	var field = formData.get("field");
	var fromTime = formData.get("fromTime");
	var toTime = formData.get("toTime");
	console.log(ipAddress);
	var scale = formData.get("scale");
	var chunksize = formData.get("chunkSize");
	console.log(field);
	console.log(fromTime);
	console.log(toTime);
	var url = `/data?field=${field}`;
	for(var ipAddress of ipAddresses) {
		if(ipAddress.trim() != '') {
			url += `&ipAddress=${ipAddress}`;
		}	
	}
	if(fromTime && fromTime.trim() != '' && toTime && toTime.trim() != '') {
		url += `&fromTime=${fromTime}&toTime=${toTime}`;
	}
	if(chunksize && chunksize.trim() != '') {
		url += `&chunkSize=${chunksize}`;
	}
	console.log(url);
	var xhr = new XMLHttpRequest();
	xhr.open('GET', url);
	xhr.onload = function() {
		var data = JSON.parse(this.responseText);
		for(var i in data) {
			for(var j in data[i])
			data[i][j].x = new Date(data[i][j].x);
		}
		displayChart(data, scale);
		console.log(data);
	}
	xhr.onreadystatechange = function() {
		console.log(this.readyState);
	}
	xhr.send();
} 

function displayChart(data, scale) {
	var datasets = [];
	var colors = [
		"blue",
		"green",
		"red",
		"yellow",
		"purple",
		"orange",
		"pink"
	];
	var i = 0;
	for(var d in data) {
		datasets.push({
			borderColor: colors[i % 7],
			pointBackgroundColor: colors[i % 7],
			lineTension: 0,
			label: d,
			data: data[d]
		});
		i++;
	}
	myChart.destroy();
	var ctx = document.getElementById("myChart").getContext('2d');
	myChart = new Chart(ctx, {
		type: 'line',
		data: {
			datasets: datasets
		},
		options: {
			legend: {
				labels: {
					usePointStyle: true,
					color: 'blue'
				}
			},
			showTooltips: false,
			scales: {
				xAxes: [{
						type: 'time',
						position: 'bottom'
				}],
				yAxes: [{
					type: scale
				}]
			}
		}
	});
}
