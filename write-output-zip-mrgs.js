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
  possible_zip_match: 0
};
setInterval(displayStats, 1000);
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

async function writeOut() {
  var mgrs_zips_db = await levelup(
    leveldown("./output/location/mgrs_zips_db")
  );

  var zips_mgrs_db = await levelup(
    leveldown("./output/location/zips_mgrs_db")
  );

  var i = 0;
  var streetInc = 0;
  let count = 0;
  let index = 0;

  let streetName = "";
  let tmpStreetData = {};
  let streetData = [];
  let mgrsData = {};
  var missing_mgrs = 0;
  var last_index = 0;
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

      if(isNaN(street.mgrs[1])) {
        street.mgrs = "0"+street.mgrs;
      }
      
      if (street.mgrs.length > 13) {
        //////////
        // MGRS DATA
        // Split into 4 parts
        let mgrsParts = ["", "", "", "", "", "", "", ""];
  
        if(!isNaN(street.mgrs[4])) {
          mgrsParts[0] = street.mgrs.substring(0, 3);
          mgrsParts[1] = street.mgrs.substring(3, 4) + 'A'; // bug in data where A is missing
          mgrsParts[2] = street.mgrs.substring(5, 6);
          mgrsParts[3] = street.mgrs.substring(6, 7);
          mgrsParts[4] = street.mgrs.substring(7, 8);
          mgrsParts[5] = street.mgrs.substring(12, 13);
          mgrsParts[6] = street.mgrs.substring(13, 14);
          mgrsParts[7] = street.mgrs.substring(14, 15);
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
        // else {
        //   mgrsParts = last_mgrs;
        //   console.log("Invalid MGRS: ", street,mgrsParts);
        
        // }

        if(street.zip_index === undefined) {
          try {
            var level3 = mgrsData[mgrsParts[0]][mgrsParts[1]][mgrsParts[2] + mgrsParts[5]];
            if(level3 != undefined) {
            var level3keys = Object.keys(level3);
            var level4 = null;
            if(level3keys.length <= 0) {
              try {
                level4 = JSON.parse((await mgrs_zips_db.get(mgrsParts[0] + mgrsParts[1] + mgrsParts[2] + mgrsParts[3]+ mgrsParts[5] + mgrsParts[6])).toString());
                
              } catch (error) {
                console.log("Error: ", error);
              }
              
            }

            if(level3keys.length > 0 || level4) {
              try {
                level4 = level4 || level3[mgrsParts[3]+ mgrsParts[6]]
                var level4keys = Object.keys(level4);
                if(level4 != undefined && level4keys.length > 0) {

                  try {
                    var level5 = level4[mgrsParts[4] + mgrsParts[7]]
                    if(level5 != undefined && level5.length > 0) {
                     
                      street.zip_index = level5[0];
                      //console.log(`level 5 zips found`, street)
                      stats.possible_zip_match++;
                    }
                    else {
                     
            
                      street.zip_index =  level4[level4keys[0]][0];
                      //console.log(`level 4 zips found`, street)
                      stats.possible_zip_match++;

                    }
                  } catch (error) {
                   
                    street.zip_index =  level4[level4keys[0]][0];
                   // console.log(`level 4 zips found`, street)
                   stats.possible_zip_match++;
                  }
                }
                else {
                 
                  
                  var l4 = level3[level3keys[0]];
                  l4keys = Object.keys(l4);
                  street.zip_index = l4[l4keys[0]][0];
                 // console.log(`level 3 zips found`, street)
                 stats.possible_zip_match++;

                }
              } catch (error) {

               
                  var l4 = level3[level3keys[0]];
                  l4keys = Object.keys(l4);
                  street.zip_index = l4[l4keys[0]][0];
                  //console.log(`level 3 zips found`, street)
                  stats.possible_zip_match++;
              }

            }
            else {
              stats.invalid_zip++;
             // console.log("Invalid ZIP level 3: ", street, level3, mgrsParts);
            }
          }
          } catch (error) {
            stats.invalid_zip++;
            //console.log("Invalid ZIP Error: ", street, mgrsParts, error);
            //console.log(error)
          }
         
        }
        if(street.zip_index !== undefined) {

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
        if(!mgrsData[mgrsParts[0]][mgrsParts[1]][mgrsParts[2] + mgrsParts[5]][mgrsParts[3] + mgrsParts[6]][mgrsParts[4] + mgrsParts[7]]) {
          mgrsData[mgrsParts[0]][mgrsParts[1]][mgrsParts[2] + mgrsParts[5]][mgrsParts[3] + mgrsParts[6]][mgrsParts[4] + mgrsParts[7]] = [street.zip_index]
        }
        else {
          // Only push unique indices
          if (
            mgrsData[mgrsParts[0]][mgrsParts[1]][mgrsParts[2] + mgrsParts[5]][mgrsParts[3] + mgrsParts[6]][mgrsParts[4] + mgrsParts[7]].indexOf(street.zip_index) < 0
          ) {
            mgrsData[mgrsParts[0]][mgrsParts[1]][mgrsParts[2] + mgrsParts[5]][mgrsParts[3] + mgrsParts[6]][mgrsParts[4] + mgrsParts[7]].push(street.zip_index);
          }
        }
        last_mgrs = mgrsParts;
        last_index = street.zip_index;
        }
        else {
          stats.invalid_zip++;
          console.log("Invalid ZIP: ", street, mgrsParts);
        }
      } else {
         mgrsData[last_mgrs[0]][last_mgrs[1]][last_mgrs[2] + last_mgrs[5]][last_mgrs[3] + last_mgrs[6]][last_mgrs[4] + last_mgrs[7]].push(street.zip_index);
        stats.missing_mgrs++;
      }
      reader.resume();
      } else if (street.name == "") {
        stats.missing_names++;
      } 
      stats.records = count++;
  });

  reader.on("close", async () => {
    const mgrsKeys = Object.keys(mgrsData);

    for (var i = 0; i < mgrsKeys.length; i++) {
      const levelOne = mgrsKeys[i];
      const secondKeys = Object.keys(mgrsData[levelOne]);


      for (var j = 0; j < secondKeys.length; j++) {
        const levelTwo = secondKeys[j];
        const ThirdKeys = Object.keys(mgrsData[levelOne][levelTwo]);


        for (var k = 0; k < ThirdKeys.length; k++) {
          const levelThree = ThirdKeys[k];
          console.log(levelOne + levelTwo + levelThree);
          const ForthKeys = Object.keys(
            mgrsData[levelOne][levelTwo][levelThree]
          );

          for (var l = 0; l < ForthKeys.length; l++) {
            const levelFour = ForthKeys[l];
            console.log(levelOne + levelTwo + levelThree[0] + levelFour[0] + levelThree[1] + levelFour[1]);
            const FithKeys = Object.keys(
              mgrsData[levelOne][levelTwo][levelThree][levelFour]
            );
            await mgrs_zips_db.put(
              levelOne + levelTwo + levelThree[0] + levelFour[0]  + levelThree[1] + levelFour[1],
              JSON.stringify(
                mgrsData[levelOne][levelTwo][levelThree][levelFour]
              )
            );
            for(var m = 0; m < FithKeys.length; m++) {
              const levelFive = FithKeys[m];
              var indexes = mgrsData[levelOne][levelTwo][levelThree][levelFour][levelFive];
              var batch = [];
              for(var n = 0; n < indexes.length; n++) {
                batch.push({
                  type: "put",
                  key: indexes[m],
                  value: levelOne + levelTwo + levelThree[0] + levelFour[0] + levelFive[0] + levelThree[1] + levelFour[1] + levelFive[1],
                });
              }
              await zips_mgrs_db.batch(batch);

            }
          }
        }
        // mgrsStream.write(`}`);
      }
      // mgrsStream.write(`}`);
    }
    console.log("\n\ndone.");
    zips_mgrs_db.close();
  });
}
setInterval(displayStats, 1000);
writeOut();
