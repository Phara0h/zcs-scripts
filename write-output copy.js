const { POSTGRESQL_CONN, LOG_INTERVAL } = require("./index");
const fs = require("fs"),
  zlib = require("zlib"),
  rl = require("readline");

const fsproms = require("fs/promises");

var levelup = require("levelup");
var leveldown = require("leveldown");

var { zip_indexes } = require("../zcs/data/zips/index.js");
var zcta = require("../zcs-location/location/zip/index.js");
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

function displayStats() {
  const heapUsed = process.memoryUsage().heapUsed / 1024 / 1024;

  process.stdout.write(
    `\rProcessed ${stats.records} records. 
    Unique names: ${stats.uniqueNames}
    missing mgrs: ${stats.missing_mgrs} 
    missing names: ${stats.missing_names} 
    found zip: ${stats.found_zip}
    invalid_zip: ${stats.invalid_zip} 
    Elapsed: ${elapsedTime()}. Heap used: ${Math.round(heapUsed * 100) / 100}MB
    `
  );
}

setInterval(displayStats, 1000);
async function writeOut() {
  var db = await levelup(leveldown("./output/location/mgrs_db"));

  var street_data_db = await levelup(
    leveldown("./output/streets/street_data_db")
  );

  console.log("\nWriting out data...");

  // remove data from any previous runs
  await fsproms.writeFile(process.cwd() + "/output/streets/names.js", "");
  await fsproms.writeFile(process.cwd() + "/output/streets/index.js", "");

  // open write streams
  const streetNamesStream = fs.createWriteStream(
    process.cwd() + "/output/streets/names.js"
  );

  const streetIndexStream = fs.createWriteStream(
    process.cwd() + "/output/streets/index.js"
  );

  streetNamesStream.write(`const street_names = [\n`);
  streetIndexStream.write(`const street_index = {\n`);

  var i = 0;
  var streetInc = 0;
  let count = 0;
  let index = 0;

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
    try {

 
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
        // Split into 4 parts

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
            // streetDataStream.write(
            //   `${index === 0 ? "" : ",\n"}${JSON.stringify(tmpStreetData)}`
            // );

            // Write unique street names to file
            // streetNamesStream.write(`${index === 0 ? "" : ",\n"}"${streetName}"`);

            // // write street indexes to file
            // streetIndexStream.write(
            //   `${index === 0 ? "" : ",\n"}"${streetName}":${Number.parseInt(index)}`
            // );

            index++;
            streetName = street.name;
            // await street_data_db.put(streetInc++, JSON.stringify(tmpStreetData));
            tmpStreetData = {};
          }

          street.zip_index = Number.parseInt(street.zip_index);

          if (!tmpStreetData[street.number]) {
            tmpStreetData[street.number] = [street.zip_index];
          }

          if (tmpStreetData[street.number].indexOf(street.zip_index) < 0) {
            tmpStreetData[street.number].push(street.zip_index);
          }

          if (!mgrsData[mgrsParts[0]]) {
            mgrsData[mgrsParts[0]] = {};
          }

          if (!mgrsData[mgrsParts[0]][mgrsParts[1]]) {
            mgrsData[mgrsParts[0]][mgrsParts[1]] = {};

            stats.uniqueNames = index + 1;
            //displayStats();
            console.log(street, row);
            console.log(mgrsParts[0] + mgrsParts[1]);
          }

          if (
            !mgrsData[mgrsParts[0]][mgrsParts[1]][mgrsParts[2] + mgrsParts[5]]
          ) {
            mgrsData[mgrsParts[0]][mgrsParts[1]][
              mgrsParts[2] + mgrsParts[5]
            ] = {};
          }
          if (
            !mgrsData[mgrsParts[0]][mgrsParts[1]][mgrsParts[2] + mgrsParts[5]][
            mgrsParts[3] + mgrsParts[6]
            ]
          ) {
            mgrsData[mgrsParts[0]][mgrsParts[1]][mgrsParts[2] + mgrsParts[5]][
              mgrsParts[3] + mgrsParts[6]
            ] = {}
          }
          var dbindex = `${index}:${street.zip_index}:${street.number}`;

          if (!mgrsData[mgrsParts[0]][mgrsParts[1]][mgrsParts[2] + mgrsParts[5]][mgrsParts[3] + mgrsParts[6]][mgrsParts[4] + mgrsParts[7]]) {
            mgrsData[mgrsParts[0]][mgrsParts[1]][mgrsParts[2] + mgrsParts[5]][mgrsParts[3] + mgrsParts[6]][mgrsParts[4] + mgrsParts[7]] = [dbindex]
          }
          else {
            // Only push unique indices
            if (
              mgrsData[mgrsParts[0]][mgrsParts[1]][mgrsParts[2] + mgrsParts[5]][mgrsParts[3] + mgrsParts[6]][mgrsParts[4] + mgrsParts[7]].indexOf(dbindex) < 0
            ) {
              mgrsData[mgrsParts[0]][mgrsParts[1]][mgrsParts[2] + mgrsParts[5]][mgrsParts[3] + mgrsParts[6]][mgrsParts[4] + mgrsParts[7]].push(dbindex);
            }
          }
          last_mgrs = mgrsParts;
        }
      } else if (street.zip_index !== undefined) {
        street.zip_index = Number.parseInt(street.zip_index);
        var dbindex = `${index}:${street.zip_index}:${street.number}`;
        mgrsData[last_mgrs[0]][last_mgrs[1]][last_mgrs[2] + last_mgrs[5]][last_mgrs[3] + last_mgrs[6]][last_mgrs[4] + last_mgrs[7]].push(dbindex);
      } else {
        stats.missing_mgrs++;
      }


      stats.records = count++;

      reader.resume();
    } else if (street.name == "") {
      stats.missing_names++;
    } else {
      stats.invalid_zip++;
      console.log("Invalid ZIP: ", street);
    }
  }catch (e) {
      console.log(e, street, row);
  }
  });


  reader.on("close", async () => {
    const mgrsKeys = Object.keys(mgrsData);

    for (var i = 0; i < mgrsKeys.length; i++) {
      const levelOne = mgrsKeys[i];
      const secondKeys = Object.keys(mgrsData[levelOne]);
        await db.put(levelOne, 0);

      for (var j = 0; j < secondKeys.length; j++) {
        const levelTwo = secondKeys[j];
        const ThirdKeys = Object.keys(mgrsData[levelOne][levelTwo]);
        await db.put(levelOne + levelTwo, 0);

        for (var k = 0; k < ThirdKeys.length; k++) {
          const levelThree = ThirdKeys[k];
          console.log(levelOne + levelTwo + levelThree);
          const ForthKeys = Object.keys(
            mgrsData[levelOne][levelTwo][levelThree]
          );
          await db.put(levelOne + levelTwo + levelThree, 0);

          for (var l = 0; l < ForthKeys.length; l++) {
            const levelFour = ForthKeys[l];
            const FithKeys = Object.keys(
              mgrsData[levelOne][levelTwo][levelThree][levelFour]
            );
            await db.put(levelOne + levelTwo + levelThree[0] + levelFour[0] + levelThree[1] + levelFour[1] , 0);
            for (var m = 0; m < FithKeys.length; m++) {
              const levelFive = FithKeys[m];
              
    
            // var batch = [];
            // for (var n = 0; n < indexes.length; n++) {
            //   batch.push({
            //     type: "put",
            //     key: indexes[n],
            //     value: levelOne + levelTwo + levelThree[0] + levelFour[0] + levelFive[0] + levelThree[1] + levelFour[1] + levelFive[1],
            //   });
            // }
            // await street_mgrs_db.batch(batch);
            await db.put(
              levelOne + levelTwo + levelThree[0] + levelFour[0] + levelFive[0] + levelThree[1] + levelFour[1] + levelFive[1],
              JSON.stringify(
                mgrsData[levelOne][levelTwo][levelThree][levelFour][levelFive]
              )
            );
           }
          }
        }
        // mgrsStream.write(`}`);
      }
      // mgrsStream.write(`}`);
    }
    // console.log("Write street data");
    // for (var i = 0; i < streetData.length; i++) {
    //   await street_data_db.put(i, streetData[i]);
    // }

//     console.log("Write unique street names to file");
//     // Write unique street names to file
//     streetNamesStream.write(`${index === 0 ? "" : ",\n"}"${streetName}"`);

//     streetNamesStream.write(`\n]\n`);
//     streetNamesStream.write(`
// Object.defineProperty(exports, "__esModule", { value: true });
// exports.street_names = street_names;
// `);
//     console.log("Write street indexes to file");
//     // write street indexes to file
//     streetIndexStream.write(
//       `${index === 0 ? "" : ",\n"}"${streetName}":${Number.parseInt(index)}`
//     );
//     streetIndexStream.write(`\n}\n`);
//     streetIndexStream.write(`
//     Object.defineProperty(exports, "__esModule", { value: true });
//     exports.street_index = street_index;
//     `);


    console.log("\n\ndone.");
    db.close();
    process.exit();
  });
}

writeOut();
