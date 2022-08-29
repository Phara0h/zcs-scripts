// *** SET THESE VARIABLES ***

module.exports = {
  DATA_FILE_LOC: "/Users/jtwhissel/Downloads/TXT/NAD_r10.txt",
  POSTGRESQL_CONN: "postgres://postgres:1234@localhost/street_data",
  LOG_INTERVAL: 10000,
};

/* ************************** 
  HOW TO RUN THIS SCRIPT
    1. Add above variables
    2. Run `node csv-transform.js`
    3  mv dumps/streets.csv dumps/streets_not_sorted.csv
    4. sort --parallel=8 -t , -k 1n streets_not_sorted.csv > streets.csv
    5. Run `node write-output.js`.
    6. Run 'node compress-files.js`.

    Note: checked in output files are placeholders only. Run script to generate data.
*/
