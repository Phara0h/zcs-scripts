const { LOG_INTERVAL } = require("./index");
const DATA_FILE_LOC = "/Users/jtwhissel/Downloads/streets.csv";
const fs = require("fs");
const fsproms = require("fs/promises");
const readline = require("readline");

const Mgrs = require("./mgrs");

// Keep track of elapsed time
let start = process.hrtime();

let elapsedTime = function () {
  const precision = 3; // 3 decimal places
  const elapsed = process.hrtime(start)[1] / 1000000; // divide by a million to get nano to milli
  return process.hrtime(start)[0] + " s, " + elapsed.toFixed(precision) + " ms";
};

const stats = {
  records: 0,
};

const ALPHA = [
  "A",
  "B",
  "C",
  "D",
  "E",
  "F",
  "G",
  "H",
  "I",
  "J",
  "K",
  "L",
  "M",
  "N",
  "O",
  "P",
  "Q",
  "R",
  "S",
  "T",
  "U",
  "V",
  "W",
  "X",
  "Y",
  "Z",
];
const ALPHA_NUMERIC = [
  "A",
  "B",
  "C",
  "D",
  "E",
  "F",
  "G",
  "H",
  "I",
  "J",
  "K",
  "L",
  "M",
  "N",
  "O",
  "P",
  "Q",
  "R",
  "S",
  "T",
  "U",
  "V",
  "W",
  "X",
  "Y",
  "Z",
  "0",
  "1",
  "2",
  "3",
  "4",
  "5",
  "6",
  "7",
  "8",
  "9",
];

function displayStats() {
  const heapUsed = process.memoryUsage().heapUsed / 1024 / 1024;

  process.stdout.write(
    `\rProcessed ${
      stats.records
    } records. Elapsed: ${elapsedTime()}. Heap used: ${
      Math.round(heapUsed * 100) / 100
    }MB`
  );
}

/**
 * Trims and limits whitespace to one and uppercase
 * @param {String} str
 * @returns
 */
function cleanUpStr(str) {
  const trimmed = str.trim();

  let resultStr = "";

  // Only allow one white space char
  for (let i = 0; i < trimmed.length; i++) {
    const char = trimmed[i];

    if (
      (char === " " &&
        resultStr.length > 0 &&
        resultStr[resultStr.length - 1] === " ") ||
      char === "\\" ||
      char === '"' ||
      char === "'" ||
      char === "`"
    ) {
      continue;
    } else {
      resultStr += char;
    }
  }

  return resultStr.toUpperCase();
}

/*
Transform NAD to combine street names and units.
Output columns:
    street_name text,
    street_number text, 
    zip_code text,
    zip_index text,
    unit text,
    mgrs text
*/
async function transformData() {
  console.log(`Processing data from ${DATA_FILE_LOC}...`);

  // remove data from any previous runs
  await fsproms.writeFile(process.cwd() + "/dumps/streets-osm.csv", "");

  // open read and write streams
  const writeStream = fs.createWriteStream(
    process.cwd() + "/dumps/streets-osm.csv"
  );

  const writeStreamNoZip = fs.createWriteStream(
    process.cwd() + "/dumps/streets-osm-no-zips.csv"
  );

  const fileStream = fs.createReadStream(DATA_FILE_LOC);
  const rl = readline.createInterface({
    input: fileStream,
  });

  let lineNumber = 0;

  rl.on("line", (line) => {
    var columns = line.toString();

    if (columns.indexOf(',"') > -1) {
      columns = columns.split(/,(?=(?:(?:[^"]*"){2})*[^"]*$)/g);
    } else {
      columns = columns.split(",");
    }
    // Skip first row
    var zipCode = columns[3] || "";
    // Get zip index by finding 'Zip_Code' in zcs zipindexes
    if (zipCode.length < 5 || zipCode.length > 10 || isNaN(zipCode)) {
      zipCode = "";
    }
    if (columns[4] && columns[5]) {
      const streetName = cleanUpStr(columns[4]);

      const streetNumber = cleanUpStr(columns[5]);

      // Building LandmkPart[22], LandmkName[23], Building[24], Floor[25], Unit[26], Room[27]
      const unit = cleanUpStr(columns[6]);

      // Generate MGRS from lat and long: Longitude[30], Latitude[31]
      var longitude = columns[1];
      var lattitude = columns[2];

      let mgrs = "";

      if (!isNaN(lattitude) && !isNaN(longitude)) {
        var tempMgrs = Mgrs.forward([
          parseFloat(longitude),
          parseFloat(lattitude),
        ]);

        let i = 0;
        if (!isNaN(tempMgrs[2])) {
          console.log(columns, tempMgrs);
        } else {
          for (i; i < tempMgrs.length; i++) {
            if (ALPHA_NUMERIC.indexOf(tempMgrs[i]) < 1) {
              continue;
            }

            mgrs += tempMgrs[i];
          }
        }
      } else {
        console.log("no lattitude or long: ", columns);
      }
      if (zipCode.length > 0 && mgrs.length > 0) {
        // Write out to dump file
        writeStream.write(
          streetName +
            "," +
            (streetNumber || "") +
            "," +
            (zipCode || "") +
            "," +
            (unit || "") +
            "," +
            (mgrs || "") +
            "\n"
        );
      } else if (mgrs.length > 0) {
        writeStreamNoZip.write(
          streetName +
            "," +
            (streetNumber || "") +
            "," +
            (zipCode || "") +
            "," +
            (unit || "") +
            "," +
            (mgrs || "") +
            "\n"
        );
      }
    }

    lineNumber++;

    // Display current stats
    if (lineNumber % LOG_INTERVAL === 0) {
      stats.records = lineNumber;
      displayStats();
    }
  });

  rl.on("close", () => {
    console.log(`\ndone.\n`);
  });
}

transformData();

/**
 * @param { string } opts.foo - just a string thats foo barish
 */
function test(opts) {
  console.log(opts.foo);
}

test;
