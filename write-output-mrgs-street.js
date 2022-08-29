const { POSTGRESQL_CONN, LOG_INTERVAL } = require("./index");
const fs = require("fs"),
  zlib = require("zlib"),
  rl = require("readline");

const fsproms = require("fs/promises");

var levelup = require("levelup");
var leveldown = require("leveldown");

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
  found_zip: 0
};
setInterval(displayStats, 1000);
function displayStats() {
  const heapUsed = process.memoryUsage().heapUsed / 1024 / 1024;

  process.stdout.write(
    `\rProcessed ${stats.records} records. 
    Unique names: ${stats.uniqueNames}
    missing mgrs: ${stats.missing_mgrs} 
    missing names: ${stats.missing_names} 
    found zip: ${stats.found_zip}
    invalid_zip: ${stats.invalid_zip} 
    Elapsed: ${elapsedTime()}. Heap used: ${Math.round(heapUsed * 100) / 100}MB`
  );
}
var zcta = require("../zcs-location/location/zip/index.js");
async function writeOut() {
  var street_mgrs_db = await levelup(
    leveldown("./output/location/street_mgrs_db")
  );

  var i = 0;
  var streetInc = 0;
  let count = 0;
  let index = 0;
  var last_index = null;
  let streetName = "";
  let tmpStreetData = {};
  let streetData = [];
  let mgrsData = {};
  var missing_mgrs = 0;
  var last_mgrs = ["", "", "", "", "", ""];
  const reader = rl.createInterface({
    input: fs.createReadStream("./dumps/streets_full.csv"),
  });
  reader.on("line", async (row) => {
    if (row.indexOf(", ") > -1) {
      row = row.split(/,(?=(?:(?:[^"]*"){2})*[^"]*$)/g);
    } else {
      row = row.split(",");
    }
    var street = {
      name: row[0],
      number: row[1],
      zip_code: row[2],
      zip_index: zip_indexes[row[2]],
      mgrs: row.pop() || "",
    };

    if (street.name != "") {
      reader.pause();



      if (isNaN(street.mgrs[1])) {
        street.mgrs = "0" + street.mgrs;
      }

      if (street.mgrs.length > 13) {
        //////////
        // MGRS DATA
        // Split into 8 parts

        let mgrsParts = ["", "", "", "", "", "", "", ""];

        if (!isNaN(street.mgrs[4])) {
          mgrsParts[0] = street.mgrs.substring(0, 3);
          mgrsParts[1] = street.mgrs.substring(3, 4) + 'A'; // bug in data where A is missing
          mgrsParts[2] = street.mgrs.substring(4, 5);
          mgrsParts[3] = street.mgrs.substring(6, 7);
          mgrsParts[4] = street.mgrs.substring(7, 8);
          mgrsParts[5] = street.mgrs.substring(11, 12);
          mgrsParts[6] = street.mgrs.substring(12, 13);
          mgrsParts[7] = street.mgrs.substring(13, 14);
        }
        else {
          mgrsParts[0] = street.mgrs.substring(0, 3);
          mgrsParts[1] = street.mgrs.substring(3, 5);
          mgrsParts[2] = street.mgrs.substring(5, 6);
          mgrsParts[3] = street.mgrs.substring(6, 7);
          mgrsParts[4] = street.mgrs.substring(7, 8);
          mgrsParts[5] = street.mgrs.substring(11, 12);
          mgrsParts[6] = street.mgrs.substring(12, 13);
          mgrsParts[7] = street.mgrs.substring(13, 14);

        }

        if (street.zip_index === undefined) {
          try {
            street.zip_code = zcta.geoMGRSSearchZip(mgrsParts[0] + mgrsParts[1] + mgrsParts[2] + mgrsParts[3] + mgrsParts[4] + mgrsParts[5] + mgrsParts[6] + mgrsParts[7]);
            street.zip_index = zip_indexes[street.zip_code];
            if (street.zip_index !== undefined) {

              stats.found_zip++;
            }

          } catch (e) {
            console.log(e, street, mgrsParts);
          }
        }
        if (street.zip_index !== undefined) {
          if (count === 0) {
            streetName = street.name;
          }

          if (street.name !== streetName) {


            index++;
            streetName = street.name;
            stats.uniqueNames = index + 1;
            tmpStreetData = {};
          }

          street.zip_index = Number.parseInt(street.zip_index);


          var dbindex = `${index}:${street.zip_index}:${street.number}`;
          await street_mgrs_db.put(dbindex, mgrsParts[0] + mgrsParts[1] + mgrsParts[2] + mgrsParts[3] + mgrsParts[4] + mgrsParts[5] + mgrsParts[6] + mgrsParts[7]);
          last_mgrs = mgrsParts;
          last_index = dbindex;
        } else {
          stats.invalid_zip++;
        }
      }
      else if (street.zip_index !== undefined) {
        street.zip_index = Number.parseInt(street.zip_index);


        var dbindex = `${index}:${street.zip_index}:${street.number}`;
        await street_mgrs_db.put(dbindex, last_mgrs[0] + last_mgrs[1] + last_mgrs[2] + last_mgrs[3] + last_mgrs[4] + last_mgrs[5] + last_mgrs[6] + last_mgrs[7]);


      } else {
        stats.missing_mgrs++;
      }

      reader.resume();
    } else if (street.name == "") {
      stats.missing_names++;
    } else {
      stats.invalid_zip++;
      console.log("Invalid ZIP: ", street);
    }
    stats.records = count++;
  });

  reader.on("close", async () => {
    console.log("\n\ndone.");
    street_mgrs_db.close();
  });
}

writeOut();
