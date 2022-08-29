const { POSTGRESQL_CONN, LOG_INTERVAL } = require("./index");

const fs = require("fs");
const fsproms = require("fs/promises");
const { Client } = require("pg");
const client = new Client({ connectionString: POSTGRESQL_CONN });
client.connect();

const QueryStream = require("pg-query-stream");

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
  highestZipCount: 0,
  highestStreetNumCount: 0
};

function displayStats() {
  const heapUsed = process.memoryUsage().heapUsed / 1024 / 1024;

  process.stdout.write(
    `\rProcessed ${stats.records} records. Unique names: ${stats.uniqueNames
    }. Highest zip count: ${stats.highestZipCount}. Highest street num count: ${stats.highestStreetNumCount}. Elapsed: ${elapsedTime()}. Heap used: ${Math.round(heapUsed * 100) / 100
    }MB`
  );
}

async function writeOut() {
  console.log("\nWriting out data...");

  // remove data from any previous runs
  await fsproms.writeFile(process.cwd() + "/output/street-names.js", "");
  await fsproms.writeFile(process.cwd() + "/output/streets-index.js", "");
  await fsproms.writeFile(process.cwd() + "/output/mgrs.js", "");
  await fsproms.writeFile(process.cwd() + "/output/stats.csv", "");
  await fsproms.rmdir(process.cwd() + "/output/street-data", { recursive: true });
  await fsproms.mkdir(process.cwd() + "/output/street-data");


  // open write streams
  const streetNamesStream = fs.createWriteStream(
    process.cwd() + "/output/street-names.js"
  );

  const streetIndexStream = fs.createWriteStream(
    process.cwd() + "/output/streets-index.js"
  );

  const streetDataStream = {
    A: fs.createWriteStream(process.cwd() + "/output/street-data/A.js"),
    B: fs.createWriteStream(process.cwd() + "/output/street-data/B.js"),
    C: fs.createWriteStream(process.cwd() + "/output/street-data/C.js"),
    D: fs.createWriteStream(process.cwd() + "/output/street-data/D.js"),
    E: fs.createWriteStream(process.cwd() + "/output/street-data/E.js"),
    F: fs.createWriteStream(process.cwd() + "/output/street-data/F.js"),
    G: fs.createWriteStream(process.cwd() + "/output/street-data/G.js"),
    H: fs.createWriteStream(process.cwd() + "/output/street-data/H.js"),
    I: fs.createWriteStream(process.cwd() + "/output/street-data/I.js"),
    J: fs.createWriteStream(process.cwd() + "/output/street-data/J.js"),
    K: fs.createWriteStream(process.cwd() + "/output/street-data/K.js"),
    L: fs.createWriteStream(process.cwd() + "/output/street-data/L.js"),
    M: fs.createWriteStream(process.cwd() + "/output/street-data/M.js"),
    N: fs.createWriteStream(process.cwd() + "/output/street-data/N.js"),
    O: fs.createWriteStream(process.cwd() + "/output/street-data/O.js"),
    P: fs.createWriteStream(process.cwd() + "/output/street-data/P.js"),
    Q: fs.createWriteStream(process.cwd() + "/output/street-data/Q.js"),
    R: fs.createWriteStream(process.cwd() + "/output/street-data/R.js"),
    S: fs.createWriteStream(process.cwd() + "/output/street-data/S.js"),
    T: fs.createWriteStream(process.cwd() + "/output/street-data/T.js"),
    U: fs.createWriteStream(process.cwd() + "/output/street-data/U.js"),
    V: fs.createWriteStream(process.cwd() + "/output/street-data/V.js"),
    W: fs.createWriteStream(process.cwd() + "/output/street-data/W.js"),
    X: fs.createWriteStream(process.cwd() + "/output/street-data/X.js"),
    Y: fs.createWriteStream(process.cwd() + "/output/street-data/Y.js"),
    Z: fs.createWriteStream(process.cwd() + "/output/street-data/Z.js"),
    OTHER: fs.createWriteStream(process.cwd() + "/output/street-data/OTHER.js"),
  };

  const statsStream = fs.createWriteStream(process.cwd() + "/output/stats.csv");

  const query = new QueryStream("select * from data order by name");
  client.query(query);

  let count = 0;
  let index = {
    A: 0,
    B: 0,
    C: 0,
    D: 0,
    E: 0,
    F: 0,
    G: 0,
    H: 0,
    I: 0,
    J: 0,
    K: 0,
    L: 0,
    M: 0,
    N: 0,
    O: 0,
    P: 0,
    Q: 0,
    R: 0,
    S: 0,
    T: 0,
    U: 0,
    V: 0,
    W: 0,
    X: 0,
    Y: 0,
    Z: 0,
    OTHER: 0,
  };

  let streetName = "";
  let streetData = {};
  let mgrsData = {};

  // Write beginning of files
  streetNamesStream.write(`module.exports = [\n`);
  streetIndexStream.write(`module.exports = {\n`);
  statsStream.write('street name, street numbers count, highest zip count\n')

  const letters = Object.keys(index)
  for (let i = 0; i < letters.length; i++) {
    streetDataStream[letters[i]].write(`module.exports = [\n`);
  }

  let letter = 'OTHER';

  for await (const street of query) {
    if (count === 0) streetName = street.name;

    if (street.name !== streetName) {
      let letter = streetName[0];
      if (index[letter] === undefined) {
        letter = 'OTHER'
      }

      streetDataStream[letter].write(
        `${index[letter] === 0 ? "" : ",\n"}${JSON.stringify(streetData)}`
      );

      // Write unique street names to file
      streetNamesStream.write(
        `${stats.uniqueNames === 0 ? "" : ",\n"}"${streetName}"`
      );

      // write street indexes to file
      streetIndexStream.write(
        `${stats.uniqueNames === 0 ? "" : ",\n"}"${streetName}":${Number.parseInt(
          index[letter]
        )}`
      );

      // Gather stats
      const streetNums = Object.keys(streetData);
      let highestZipCount = 0;

      for (let i = 0; i < streetNums.length; i++) {
        const zips = streetData[streetNums[i]];
        if (zips.length > highestZipCount) highestZipCount = zips.length;
      }

      // write out stats - street name, street numbers count, highest zip count
      statsStream.write(
        `${streetName},${streetNums.length},${highestZipCount}\n`
      );

      if (highestZipCount > stats.highestZipCount) stats.highestZipCount = highestZipCount
      if (streetNums.length > stats.highestStreetNumCount) stats.highestStreetNumCount = streetNums.length

      // Reset
      index[letter]++;
      stats.uniqueNames++
      streetName = street.name;
      streetData = {};
    }

    street.zip_index = Number.parseInt(street.zip_index);

    if (!streetData[street.number]) {
      streetData[street.number] = [street.zip_index];
    }

    if (streetData[street.number].indexOf(street.zip_index) < 0) {
      streetData[street.number].push(street.zip_index);
    }

    //////////
    // MGRS DATA
    // Split into 4 parts
    // let mgrsParts = ["", "", "", ""];

    // let j;
    // for (j = 0; j < street.mgrs.length; j++) {
    //   if (j < 3) {
    //     mgrsParts[0] += street.mgrs[j];
    //   } else if (j >= 3 && j < 5) {
    //     mgrsParts[1] += street.mgrs[j];
    //   } else if (j >= 5 && j < 10) {
    //     mgrsParts[2] += street.mgrs[j];
    //   } else if (j >= 10 && j < 15) {
    //     mgrsParts[3] += street.mgrs[j];
    //   }
    // }

    // if (!mgrsData[mgrsParts[0]]) {
    //   mgrsData[mgrsParts[0]] = {};
    // }

    // if (!mgrsData[mgrsParts[0]][mgrsParts[1]]) {
    //   mgrsData[mgrsParts[0]][mgrsParts[1]] = {};
    // }

    // if (!mgrsData[mgrsParts[0]][mgrsParts[1]][mgrsParts[2]]) {
    //   mgrsData[mgrsParts[0]][mgrsParts[1]][mgrsParts[2]] = {};
    // }

    // if (!mgrsData[mgrsParts[0]][mgrsParts[1]][mgrsParts[2]][mgrsParts[3]]) {
    //   mgrsData[mgrsParts[0]][mgrsParts[1]][mgrsParts[2]][mgrsParts[3]] = [
    //     index,
    //   ];
    // } else {
    //   // Only push unique indices
    //   if (
    //     mgrsData[mgrsParts[0]][mgrsParts[1]][mgrsParts[2]][
    //       mgrsParts[3]
    //     ].indexOf(index) < 0
    //   ) {
    //     mgrsData[mgrsParts[0]][mgrsParts[1]][mgrsParts[2]][mgrsParts[3]].push(
    //       index
    //     );
    //   }
    // }

    count++;

    // Display current stats
    if (count % LOG_INTERVAL === 0) {
      stats.records = count;
      displayStats();
    }
  }

  stats.records = count;
  displayStats();
  client.end();

  // Write street data to file one last time
  streetDataStream[letter].write(`${JSON.stringify(streetData)}\n`);

  // Write unique street names to file
  streetNamesStream.write(`${index === 0 ? "" : ","}"${streetName}"`);

  // write street indexes to file
  streetIndexStream.write(
    `${index === 0 ? "" : ","}"${streetName}":${Number.parseInt(index)}`
  );

  streetNamesStream.write(`\n]`);
  streetNamesStream.end();

  streetIndexStream.write(`\n}`);
  streetIndexStream.end();

  for (let i = 0; i < letters.length; i++) {
    streetDataStream[letters[i]].write(`]`);
    streetDataStream[letters[i]].end();
  }

  // Write MGRS data
  const mgrsStream = fs.createWriteStream(process.cwd() + "/output/mgrs.js");

  mgrsStream.write(`module.exports = {\n`);

  const mgrsKeys = Object.keys(mgrsData);

  let i = 0;
  let j = 0;
  let k = 0;
  for (i = 0; i < mgrsKeys.length; i++) {
    const levelOne = mgrsKeys[i];
    const secondKeys = Object.keys(mgrsData[levelOne]);

    mgrsStream.write(`${i === 0 ? "" : ","}"${levelOne}":{`);

    for (j = 0; j < secondKeys.length; j++) {
      const levelTwo = secondKeys[j];
      const ThirdKeys = Object.keys(mgrsData[levelOne][levelTwo]);

      mgrsStream.write(`${j === 0 ? "" : ","}"${levelTwo}":{`);

      for (k = 0; k < ThirdKeys.length; k++) {
        const levelThree = ThirdKeys[k];

        mgrsStream.write(
          `${k === 0 ? "" : ","}"${levelThree}":${JSON.stringify(
            mgrsData[levelOne][levelTwo][levelThree]
          )}`
        );
      }
      mgrsStream.write(`}`);
    }
    mgrsStream.write(`}`);
  }
  mgrsStream.write(`}`);
  mgrsStream.end();

  console.log("\n\ndone.");
}

writeOut();
