async function main() {
  const { LOG_INTERVAL } = require("./index");
  const DATA_FILE_LOC = "/Users/jtwhissel/Downloads/zips.csv";
  const DATA_FILE_LOC2 = "/Users/jtwhissel/Downloads/uszips.csv";
  const fs = require("fs");
  const fsproms = require("fs/promises");
  const readline = require("readline");
  var gzips = [];
  // Keep track of elapsed time
  let start = process.hrtime();
  // remove data from any previous runs
  await fsproms.writeFile(process.cwd() + "/output/zips/zips.js", "");
  await fsproms.writeFile(process.cwd() + "/output/zips/index.js", "");

  const zipIndexesStream = fs.createWriteStream(
    process.cwd() + "/output/zips/index.js"
  );
  zipIndexesStream.write(`const zip_indexes = {\n`);

  const zipsStream = fs.createWriteStream(
    process.cwd() + "/output/zips/zips.js"
  );
  zipsStream.write(`const zips = [\n`);
  let elapsedTime = function () {
    const precision = 3; // 3 decimal places
    const elapsed = process.hrtime(start)[1] / 1000000; // divide by a million to get nano to milli
    return (
      process.hrtime(start)[0] + " s, " + elapsed.toFixed(precision) + " ms"
    );
  };

  const stats = {
    records: 0,
  };
  let index = 0;
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

  var city_names = [];
  var state_names = [];
  var state_index = {};
  var city_index = {};
  var state_data = [];
  var city_data = [];
  var zip_data = [];
  async function transformData2() {
    console.log(`Processing data from ${DATA_FILE_LOC2}...`);

    const fileStream = fs.createReadStream(DATA_FILE_LOC2);
    const rl = readline.createInterface({
      input: fileStream,
    });

    let lineNumber = 0;
    let count = 0;
    var last_zip = 0;
    var last_city = 0;

    var zip = null;
    var city = null;
    var state = null;

    rl.on("line", async (line) => {
      rl.pause();
      var columns = line.toString().split(",");
      zip = columns[0].replace(/\"/g, "");
      city = columns[3].replace(/\"/g, "").toUpperCase();
      state = columns[4].replace(/\"/g, "").toUpperCase();

      if (count > 0) {
        if (city_names.indexOf(city) === -1) {
          city_names.push(city);
        }

        if (state_names.indexOf(state) === -1) {
          state_names.push(state);
        }
        if (gzips.indexOf(zip) === -1) {
          zipsStream.write(`${index === 0 ? "" : ",\n"}"${zip}"`);

          zipIndexesStream.write(
            `${index === 0 ? "" : ",\n"}"${zip}":${Number.parseInt(index)}`
          );
          index++;
          gzips.push(zip);
        }
      }
      count++;
      lineNumber++;

      // Display current stats
      if (lineNumber % LOG_INTERVAL === 0) {
        stats.records = lineNumber;
        displayStats();
      }
      rl.resume();
    });

    rl.on("close", async () => {
      console.log("Write unique street names to file");
      // Write unique street names to file

      console.log("Write street indexes to file");
      // write street indexes to file

      zipsStream.write(`\n]\n`);
      zipIndexesStream.write(`\n}\n`);
      zipsStream.write(`
Object.defineProperty(exports, "__esModule", { value: true });
exports.zips = zips;
`);
      zipIndexesStream.write(`
Object.defineProperty(exports, "__esModule", { value: true });
exports.zip_indexes = zip_indexes;
`);
      await createCityStateData();
    });
  }

  async function transformData() {
    console.log(`Processing data from ${DATA_FILE_LOC}...`);

    const fileStream = fs.createReadStream(DATA_FILE_LOC);
    const rl = readline.createInterface({
      input: fileStream,
    });

    let lineNumber = 0;
    let count = 0;
    var last_zip = 0;
    var last_city = 0;

    var zip = null;
    var city = null;
    var state = null;

    rl.on("line", async (line) => {
      rl.pause();
      var columns = line.toString().split(",");
      zip = columns[4];
      city = columns[7];
      state = columns[8];

      if (count === 0) {
        last_zip = zip;
      } else if (columns[8] !== "") {
        if (city_names.indexOf(city) === -1) {
          city_names.push(city);
        }
        if (state_names.indexOf(state) === -1) {
          state_names.push(state);
        }

        if (zip !== last_zip) {
          // streetDataStream.write(
          //   `${index === 0 ? "" : ",\n"}${JSON.stringify(tmpStreetData)}`
          // );

          // Write unique street names to file
          zipsStream.write(`${index === 0 ? "" : ",\n"}"${zip}"`);

          // write street indexes to file
          zipIndexesStream.write(
            `${index === 0 ? "" : ",\n"}"${zip}":${Number.parseInt(index)}`
          );
          gzips.push(zip);
          index++;
          last_zip = zip;
        }
      }
      count++;
      lineNumber++;

      // Display current stats
      if (lineNumber % LOG_INTERVAL === 0) {
        stats.records = lineNumber;
        displayStats();
      }
      rl.resume();
    });

    rl.on("close", async () => {
      await transformData2();
    });
  }
  async function createCityStateData2() {
    console.log(`Processing data from ${DATA_FILE_LOC2}...`);

    let lineNumber = 0;
    let count = 0;
    var { zip_indexes } = require(process.cwd() + "/output/zips/index.js");

    const fileStream = fs.createReadStream(DATA_FILE_LOC2);
    const rl = readline.createInterface({
      input: fileStream,
    });

    rl.on("line", (line) => {
      rl.pause();
      //console.log(line);
      //console.log(zip_indexes[zip]);
      var columns = line.toString().split(",");

      zip = columns[0].replace(/\"/g, "");
      city = columns[3].replace(/\"/g, "").toUpperCase();
      state = columns[4].replace(/\"/g, "").toUpperCase();
      if (count > 0) {
        //console.log(zip, city, state);
        var zip_index = Number.parseInt(zip_indexes[zip]);
        if (isNaN(zip_index)) {
          console.log(zip, city, state);
        }
        //console.log(state_index);
        var curr_state_index = state_index[state];
        var curr_city_index = city_index[city];

        if (
          curr_city_index == null ||
          curr_state_index == null ||
          zip_index == null
        ) {
          console.log(zip, city, state);
        }
        zip_data[zip_index] = [curr_state_index, curr_city_index];

        if (city_data[curr_city_index] === undefined) {
          city_data[curr_city_index] = [];
        }
        if (city_data[curr_city_index].indexOf(zip_index) === -1) {
          city_data[curr_city_index].push(curr_state_index);
        }

        if (state_data[curr_state_index] === undefined) {
          state_data[curr_state_index] = {};
        }
        if (state_data[curr_state_index][curr_city_index] === undefined) {
          state_data[curr_state_index][curr_city_index] = [];
        }
        if (
          state_data[curr_state_index][curr_city_index].indexOf(zip_index) ===
          -1
        ) {
          //console.log(zip_index);
          state_data[curr_state_index][curr_city_index].push(zip_index);
        }
      }
      count++;
      lineNumber++;

      // Display current stats
      if (lineNumber % LOG_INTERVAL === 0) {
        stats.records = lineNumber;
        displayStats();
      }
      rl.resume();
    });

    rl.on("close", () => {
      console.log("Write state data to file");
      // Write unique street names to file

      const stateDataStream = fs.createWriteStream(
        process.cwd() + "/output/states/data.js"
      );
      stateDataStream.write(`const state_data = [\n`);
      for (var i = 0; i < state_data.length; i++) {
        var state = state_data[i];
        var city_indexs = Object.keys(state);
        stateDataStream.write(`{\n`);
        for (var j = 0; j < city_indexs.length; j++) {
          var city_index2 = city_indexs[j];
          var city_data2 = state[city_index2];
          stateDataStream.write(
            `${city_index2}: ${JSON.stringify(city_data2)},\n`
          );
        }
        stateDataStream.write(`}${i + 1 < state_data.length ? "," : ""}\n`);
      }

      stateDataStream.write(`\n]\n`);
      stateDataStream.write(`
Object.defineProperty(exports, "__esModule", { value: true });
exports.state_data = state_data;
`);

      console.log("Write city data to file");
      // Write unique street names to file

      const cityDataStream = fs.createWriteStream(
        process.cwd() + "/output/cities/data.js"
      );

      cityDataStream.write(`const city_data = [\n`);
      for (var i = 0; i < city_data.length; i++) {
        cityDataStream.write(
          `${i === 0 ? "" : ",\n"}${JSON.stringify(city_data[i])}`
        );
      }

      cityDataStream.write(`\n]\n`);
      cityDataStream.write(`
Object.defineProperty(exports, "__esModule", { value: true });
exports.city_data = city_data;
`);

      console.log("Write city names to file");

      const cityNamesStream = fs.createWriteStream(
        process.cwd() + "/output/cities/names.js"
      );

      cityNamesStream.write(`const city_names = [\n`);
      for (var i = 0; i < city_names.length; i++) {
        cityNamesStream.write(
          `${i === 0 ? "" : ",\n"}${JSON.stringify(city_names[i])}`
        );
      }

      cityNamesStream.write(`\n]\n`);
      cityNamesStream.write(`
Object.defineProperty(exports, "__esModule", { value: true });
exports.city_names = city_names;
`);

      console.log("Write state names to file");
      const stateNamesStream = fs.createWriteStream(
        process.cwd() + "/output/states/names.js"
      );

      stateNamesStream.write(`const state_names = [\n`);
      for (var i = 0; i < state_names.length; i++) {
        stateNamesStream.write(
          `${i === 0 ? "" : ",\n"}${JSON.stringify(state_names[i])}`
        );
      }

      stateNamesStream.write(`\n]\n`);
      stateNamesStream.write(`
Object.defineProperty(exports, "__esModule", { value: true });
exports.state_names = state_names;
`);

      console.log("Write city indexes to file");
      const cityIndexStream = fs.createWriteStream(
        process.cwd() + "/output/cities/index.js"
      );

      cityIndexStream.write(`const city_indexes = {\n`);
      for (var i = 0; i < city_names.length; i++) {
        cityIndexStream.write(
          `${i === 0 ? "" : ",\n"}${JSON.stringify(city_names[i])}: ${i}`
        );
      }

      cityIndexStream.write(`\n}\n`);
      cityIndexStream.write(`
Object.defineProperty(exports, "__esModule", { value: true });
exports.city_indexes = city_indexes;  
`);

      console.log("Write state indexes to file");

      const stateIndexStream = fs.createWriteStream(
        process.cwd() + "/output/states/index.js"
      );

      stateIndexStream.write(`const state_indexes = {\n`);
      for (var i = 0; i < state_names.length; i++) {
        stateIndexStream.write(
          `${i === 0 ? "" : ",\n"}${JSON.stringify(state_names[i])}: ${i}`
        );
      }

      stateIndexStream.write(`\n}\n`);
      stateIndexStream.write(`
Object.defineProperty(exports, "__esModule", { value: true });
exports.state_indexes = state_indexes;
`);

      console.log("Write zip data to file");
      const zipDataStream = fs.createWriteStream(
        process.cwd() + "/output/zips/data.js"
      );
      zipDataStream.write("const zip_data = [\n");
      for (var i = 0; i < zip_data.length; i++) {
        zipDataStream.write(
          `${i === 0 ? "" : ",\n"}${JSON.stringify(zip_data[i])}`
        );
      }
      zipDataStream.write("\n]\n");
      zipDataStream.write(`
Object.defineProperty(exports, "__esModule", { value: true });
exports.zip_data = zip_data;
`);

      console.log(`\ndone.\n`);
    });
  }
  async function createCityStateData() {
    console.log(`Processing data from ${DATA_FILE_LOC}...`);

    await fsproms.writeFile(process.cwd() + "/output/cities/data.js", "");
    await fsproms.writeFile(process.cwd() + "/output/cities/index.js", "");
    await fsproms.writeFile(process.cwd() + "/output/cities/names.js", "");

    await fsproms.writeFile(process.cwd() + "/output/states/data.js", "");

    let lineNumber = 0;
    let count = 0;
    var { zip_indexes } = require(process.cwd() + "/output/zips/index.js");
    city_names = city_names.sort((a, b) => a.localeCompare(b));
    state_names = state_names.sort((a, b) => a.localeCompare(b));

    for (var i = 0; i < state_names.length; i++) {
      state_index[state_names[i]] = i;
    }
    //console.log(state_index, state_index["PR"]);
    for (var i = 0; i < city_names.length; i++) {
      city_index[city_names[i]] = i;
    }

    const fileStream = fs.createReadStream(DATA_FILE_LOC);
    const rl = readline.createInterface({
      input: fileStream,
    });

    rl.on("line", (line) => {
      rl.pause();
      //console.log(line);
      //console.log(zip_indexes[zip]);
      var columns = line.toString().split(",");
      if (count > 0 && columns[8] !== "") {
        var zip = columns[4];
        var city = columns[7];
        var state = columns[8];
        //console.log(zip, city, state);
        var zip_index = Number.parseInt(zip_indexes[zip]);
        if (zip_index == null) {
          console.log(zip, city, state);
        }

        //console.log(state_index);
        var curr_state_index = state_index[state];
        var curr_city_index = city_index[city];
        if (
          curr_city_index == null ||
          curr_state_index == null ||
          zip_index == null
        ) {
          console.log(
            zip,
            city,
            state,
            zip_index,
            curr_city_index,
            curr_state_index
          );
        }
        zip_data[zip_index] = [curr_state_index, curr_city_index];

        if (city_data[curr_city_index] === undefined) {
          city_data[curr_city_index] = [];
        }

        if (state_data[curr_state_index] === undefined) {
          state_data[curr_state_index] = {};
        }
        if (state_data[curr_state_index][curr_city_index] === undefined) {
          state_data[curr_state_index][curr_city_index] = [];
        }
        if (
          state_data[curr_state_index][curr_city_index].indexOf(zip_index) ===
          -1
        ) {
          state_data[curr_state_index][curr_city_index].push(zip_index);
        }
        if (city_data[curr_city_index].indexOf(zip_index) === -1) {
          city_data[curr_city_index].push(curr_state_index);
        }
      }
      count++;
      lineNumber++;

      // Display current stats
      if (lineNumber % LOG_INTERVAL === 0) {
        stats.records = lineNumber;
        displayStats();
      }
      rl.resume();
    });

    rl.on("close", async () => {
      await createCityStateData2();
    });
  }

  transformData();
}
main();
