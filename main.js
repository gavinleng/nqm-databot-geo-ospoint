/*
 * Created by G on 24/10/2016.
 */


const OSPoint = require('ospoint');
const fs = require("fs");
const Converter = require("csvtojson").Converter;

function databot(input, output, context) {
	output.progress(0);
	
	if (!input.dataInId || !input.outPath) {
		output.debug("invalid arguments - please supply dataInId, outPath");
		process.exit(0);
	}
	
	const dataInId = input.dataInId;
	const outPath = input.outPath;
	
	const writeStream = fs.createWriteStream(outPath);
	const converter = new Converter({});
	
	const api = context.tdxApi;
	
	api.getRawFile(dataInId).pipe(converter)
		.on("record_parsed", function(jsonObj) {
			const singleEntry = jsonObj;
		
			var singleData = {"type": "Feature","properties": {}, "geometry": {"type": "Point", "coordinates": []}};
			
			const Northings = singleEntry.Northing;
			const Eastings = singleEntry.Easting;
			
			if (Northings != 0 && Eastings != 0) {
				const thisPoint = new OSPoint(Northings, Eastings);
				const wgs84 = thisPoint.toWGS84();
				
				singleEntry.geoStatus = "OK";
				singleData.geometry.coordinates = [wgs84.longitude, wgs84.latitude];
			} else {
				output.debug("ZERO");
				output.debug("error: data status is not ok and is ZERO"); // usually: ZERO is given, indicating wrong address format
				singleEntry.geoStatus = "ZERO";
			}
			
			singleData.properties = singleEntry;
			
			writeStream.write(JSON.stringify(singleData) + "\n");
		});
	
	output.progress(100);
}

var input = require("nqm-databot-utils").input;
input.pipe(databot);
