const { POSTGRESQL_CONN, LOG_INTERVAL } = require("./index");
const fs = require("fs"),
  zlib = require("zlib"),
  rl = require("readline");

const fsproms = require("fs/promises");

var zcats = require("/Users/jtwhissel/Downloads/zip-code-tabulation-area.json");

var { zip_indexes } = require("../zcs/data/zips/index.js");

// Keep track of elapsed time
let start = process.hrtime();

let elapsedTime = function () {
  const precision = 3; // 3 decimal places
  const elapsed = process.hrtime(start)[1] / 1000000; // divide by a million to get nano to milli
  return process.hrtime(start)[0] + " s, " + elapsed.toFixed(precision) + " ms";
};

const stats = {
  records: 0,
  uniqueNames: 0,
  missing_mgrs: 0,
  invalid_zip: 0,
  missing_names: 0,
  possible_zip_match: 0
};

function displayStats() {
  const heapUsed = process.memoryUsage().heapUsed / 1024 / 1024;

  process.stdout.write(
    `\rProcessed ${stats.records} records. Unique names: ${
      stats.uniqueNames
    } missing mgrs: ${stats.missing_mgrs} 
    } invalid_zip: ${stats.invalid_zip} 
    missing names: ${stats.missing_names} 
    possible zip match: ${stats.possible_zip_match}
    Elapsed: ${elapsedTime()}. Heap used: ${Math.round(heapUsed * 100) / 100}MB`
  );
}
var zips = {};
async function writeOut() {
  // remove data from any previous runs
  await fsproms.writeFile(process.cwd() + "/output/zips/zcta-bounds.json", "");

  // open read and write streams
  const writeStream = fs.createWriteStream(
    process.cwd() + "/output/zips/zcta-bounds.json"
  );


  for(let i = 0; i < zcats.features.length; i++) {

    zips[zcats.features[i].properties[`ZCTA5CE20`]] =zcats.features[i].geometry.coordinates;
  }
  writeStream.write(JSON.stringify(zips));
}
//setInterval(displayStats, 1000);
writeOut();
