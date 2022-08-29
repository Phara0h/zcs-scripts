const { DATA_FILE_LOC, LOG_INTERVAL } = require("./index");

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

// zcs data
const zipindexes = require("../../data/zips/index.js");

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
  await fsproms.writeFile(process.cwd() + "/dumps/streets.csv", "");

  // open read and write streams
  const writeStream = fs.createWriteStream(
    process.cwd() + "/dumps/streets.csv"
  );

  const fileStream = fs.createReadStream(DATA_FILE_LOC);
  const rl = readline.createInterface({
    input: fileStream,
  });

  let lineNumber = 0;

  rl.on("line", (line) => {
    var columns = line.toString();
    if (columns.indexOf(",DC,") === -1) {
      if (columns.indexOf(',"') > -1) {
        columns = columns.split(/,(?=(?:(?:[^"]*"){2})*[^"]*$)/g);
      } else {
        columns = columns.split(",");
      }
      // Skip first row
      if (lineNumber > 0) {
        // Get zip index by finding 'Zip_Code' in zcs zipindexes
        const zipCode = columns[7];

        // Combine street name: StN_PreMod[11], StN_PreDir[12], StN_PreTyp[13], StN_PreSep[14], StreetName[15], StN_PosTyp[16], StN_PosDir[17], StN_PosMod[18]
        const streetName = cleanUpStr(
          (columns[11] + " " || "") +
            (columns[12] + " " || "") +
            (columns[13] + " " || "") +
            (columns[14] + " " || "") +
            (columns[15] + " " || "") +
            (columns[16] + " " || "") +
            (columns[17] + " " || "") +
            (columns[18] + " " || "")
        );

        // Combine street number: AddNum_Pre[19], Add_Number[20], AddNum_Suf[21]
        const streetNumber = cleanUpStr(
          (columns[19] + " " || "") +
            (columns[20] + " " || "") +
            (columns[21] + " " || "")
        );

        // Building LandmkPart[22], LandmkName[23], Building[24], Floor[25], Unit[26], Room[27]
        const unit = cleanUpStr(
          (columns[22] + " " || "") +
            (columns[23] + " " || "") +
            (columns[24] ? "Building " + columns[24] + " " : "") +
            (columns[25] ? "Floor " + columns[25] + " " : "") +
            (columns[26] && columns[26] != "0"
              ? "Unit " + columns[26] + " "
              : "") +
            (columns[27] ? "Room " + columns[27] + " " : "")
        );

        // Generate MGRS from lat and long: Longitude[30], Latitude[31]
        var longitude = columns[30];
        var lattitude = columns[31];

        let mgrs = "";

        if (!isNaN(lattitude) && !isNaN(longitude)) {
          var tempMgrs = Mgrs.forward([longitude, lattitude]);

          let i = 0;
          if (!isNaN(tempMgrs[2])) {
            if (columns[32]) {
              columns[32] = columns[32].replace(/ /g, "");
              if (ALPHA.indexOf(columns[32][2]) === -1) {
                console.log("invalid mgrs: ", tempMgrs, columns);
              } else {
                mgrs = columns[32];
              }
            }
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

        if (streetName && zipIndex) {
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
        }
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
