const { LOG_INTERVAL } = require("./index");
const DATA_FILE_LOC = process.argv[2];
const fs = require("fs");
const fsproms = require("fs/promises");
const readline = require("readline");
var file_loc = `/dumps/streets-tiger/${DATA_FILE_LOC.split("/").pop()}`;
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
  await fsproms.writeFile(process.cwd() + file_loc, "");

  // open read and write streams
  const writeStream = fs.createWriteStream(process.cwd() + file_loc);

  const fileStream = fs.createReadStream(DATA_FILE_LOC);
  const rl = readline.createInterface({
    input: fileStream,
  });

  let lineNumber = 0;

  rl.on("line", (line) => {
    var columns = line.toString();

    columns = columns.split(";");
    //console.log(columns);
    var lineobj = {
      numbers: [],
      interpolation: columns[2],
      street: columns[3],
      city: columns[4],
      state: columns[5],
      postcode: columns[6],
      geometry: columns[7],
    };
    if(lineobj.state != `PR`) {
    columns[0] = Number(columns[0]);
    columns[1] = Number(columns[1]);

    if (Number(columns[1]) > Number(columns[0])) {
      for (
        var i = Number(columns[0]);
        i < Number(columns[1]);
        i += lineobj.interpolation === "all" ? 1 : 2
      ) {
        // console.log(i);
        lineobj.numbers.push(i);
      }
    } else {
      for (
        var i = Number(columns[1]);
        i < Number(columns[0]);
        i += lineobj.interpolation === "all" ? 1 : 2
      ) {
        // console.log(i);
        lineobj.numbers.push(i);
      }
    }
    if (lineobj.geometry && lineobj.geometry !== "geometry") {
      if (lineobj.geometry.indexOf("LINESTRING(") > -1) {
        lineobj.geometry = lineobj.geometry
          .replace("LINESTRING(", "")
          .replace(")", "")
          .split(",");
      } else {
        //console.log(lineobj.geometry);
      }
      var points = [];
      var last_point = [0, 0];
      for (var i = 0; i < lineobj.geometry.length; i++) {
        var cords = lineobj.geometry[i].split(" ");
        var longitude = parseFloat(cords[0]);
        var lattitude = parseFloat(cords[1]);
        if (longitude !== last_point[0] || lattitude !== last_point[1]) {
          last_point[0] = longitude;
          last_point[1] = lattitude;
          // console.log(last_point, longitude, lattitude);
          // console.log("sdfsdf");
          points.push([longitude, lattitude]);
        } else {
          // console.log(
          //   longitude,
          //   lattitude,
          //   last_point,
          //   longitude !== last_point[0] && lattitude !== last_point[1]
          // );
        }
      }

      var newpoints =
        points.length > 1
          ? interpolateLineRange(points, lineobj.numbers.length)
          : points;
      // console.log(newpoints, points, lineobj);
      for (var i = 0; i < lineobj.numbers.length; i++) {
        var number = lineobj.numbers[i];
        //console.log(newpoints[i]);

        const streetName = cleanUpStr(lineobj.street);

        // Generate MGRS from lat and long: Longitude[30], Latitude[31]
        var longitude;
        var lattitude;
        if (newpoints[i]) {
          longitude = newpoints[i][0];
          lattitude = newpoints[i][1];
        } else {
          longitude = newpoints[0][0];
          lattitude = newpoints[0][1];
          console.log(newpoints, points, lineobj);
        }

        let mgrs = "";

        // if (!isNaN(lattitude) && !isNaN(longitude)) {
        var tempMgrs = Mgrs.forward([longitude, lattitude]);

        if (!isNaN(tempMgrs[2])) {
          console.log(columns, tempMgrs);
        } else {
          for (var j = 0; j < tempMgrs.length; j++) {
            if (ALPHA_NUMERIC.indexOf(tempMgrs[j]) < 1) {
              continue;
            }

            mgrs += tempMgrs[j];
          }
        }
        // } else {
        //   console.log("no lattitude or long: ", newpoints, points, i);
        // }
        if (mgrs.length > 0) {
          // Write out to dump file
          writeStream.write(
            streetName +
              "," +
              (number || "") +
              "," +
              (lineobj.postcode || "") +
              "," +
              "" +
              "," +
              (mgrs || "") +
              "\n"
          );
        } else {
          console.log("no mgrs: ", columns);
        }
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
 * @param {Point} pt1
 * @param {Point} pt1
 * @return number The Euclidean distance between `pt1` and `pt2`.
 */
function distance(pt1, pt2) {
  var deltaX = pt1[0] - pt2[0];
  var deltaY = pt1[1] - pt2[1];
  return Math.sqrt(deltaX * deltaX + deltaY * deltaY);
}

/**
 * @param {Point} point The Point object to offset.
 * @param {number} dx The delta-x of the line segment from which `point` will
 *    be offset.
 * @param {number} dy The delta-y of the line segment from which `point` will
 *    be offset.
 * @param {number} distRatio The quotient of the distance to offset `point`
 *    by and the distance of the line segment from which it is being offset.
 */
function offsetPoint(point, dx, dy, distRatio) {
  return [point[0] - dy * distRatio, point[1] + dx * distRatio];
}

/**
 * @param {array of Point} ctrlPoints The vertices of the (multi-segment) line
 *      to be interpolate along.
 * @param {int} number The number of points to interpolate along the line; this
 *      includes the endpoints, and has an effective minimum value of 2 (if a
 *      smaller number is given, then the endpoints will still be returned).
 * @param {number} [offsetDist] An optional perpendicular distance to offset
 *      each point from the line-segment it would otherwise lie on.
 * @param {int} [minGap] An optional minimum gap to maintain between subsequent
 *      interpolated points; if the projected gap between subsequent points for
 *      a set of `number` points is lower than this value, `number` will be
 *      decreased to a suitable value.
 */
function interpolateLineRange(ctrlPoints, number, offsetDist, minGap) {
  minGap = minGap || 0;
  offsetDist = offsetDist || 0;

  // Calculate path distance from each control point (vertex) to the beginning
  // of the line, and also the ratio of `offsetDist` to the length of every
  // line segment, for use in computing offsets.
  var totalDist = 0;
  var ctrlPtDists = [0];
  var ptOffsetRatios = [];
  for (var pt = 1; pt < ctrlPoints.length; pt++) {
    var dist = distance(ctrlPoints[pt], ctrlPoints[pt - 1]);
    totalDist += dist;
    ptOffsetRatios.push(offsetDist / dist);
    ctrlPtDists.push(totalDist);
  }

  if (totalDist / (number - 1) < minGap) {
    number = totalDist / minGap + 1;
  }

  // Variables used to control interpolation.
  var step = totalDist / (number - 1);
  var interpPoints = [
    offsetPoint(
      ctrlPoints[0],
      ctrlPoints[1][0] - ctrlPoints[0][0],
      ctrlPoints[1][1] - ctrlPoints[0][1],
      ptOffsetRatios[0]
    ),
  ];
  var prevCtrlPtInd = 0;
  var currDist = 0;
  var currPoint = ctrlPoints[0];
  var nextDist = step;

  for (pt = 0; pt < number - 2; pt++) {
    // Find the segment in which the next interpolated point lies.
    while (nextDist > ctrlPtDists[prevCtrlPtInd + 1]) {
      prevCtrlPtInd++;
      currDist = ctrlPtDists[prevCtrlPtInd];
      currPoint = ctrlPoints[prevCtrlPtInd];
    }

    // Interpolate the coordinates of the next point along the current segment.
    var remainingDist = nextDist - currDist;
    var ctrlPtsDeltaX =
      ctrlPoints[prevCtrlPtInd + 1][0] - ctrlPoints[prevCtrlPtInd][0];
    var ctrlPtsDeltaY =
      ctrlPoints[prevCtrlPtInd + 1][1] - ctrlPoints[prevCtrlPtInd][1];
    var ctrlPtsDist =
      ctrlPtDists[prevCtrlPtInd + 1] - ctrlPtDists[prevCtrlPtInd];
    var distRatio = remainingDist / ctrlPtsDist;

    currPoint = [
      currPoint[0] + ctrlPtsDeltaX * distRatio,
      currPoint[1] + ctrlPtsDeltaY * distRatio,
    ];

    // Offset currPoint according to `offsetDist`.
    var offsetRatio = offsetDist / ctrlPtsDist;
    interpPoints.push(
      offsetPoint(
        currPoint,
        ctrlPtsDeltaX,
        ctrlPtsDeltaY,
        ptOffsetRatios[prevCtrlPtInd]
      )
    );

    currDist = nextDist;
    nextDist += step;
  }

  interpPoints.push(
    offsetPoint(
      ctrlPoints[ctrlPoints.length - 1],
      ctrlPoints[ctrlPoints.length - 1][0] -
        ctrlPoints[ctrlPoints.length - 2][0],
      ctrlPoints[ctrlPoints.length - 1][1] -
        ctrlPoints[ctrlPoints.length - 2][1],
      ptOffsetRatios[ptOffsetRatios.length - 1]
    )
  );
  return interpPoints;
}

// function interpolate(d, f, a, b) {
//   var A = Math.sin((1 - f) * d) / Math.sin(d);
//   var B = Math.sin(f * d) / Math.sin(d);
//   var X =
//     A * Math.cos(a.lat) * Math.cos(a.lon) +
//     B * Math.cos(b.lat) * Math.cos(b.lon);
//   var Y =
//     A * Math.cos(a.lat) * Math.sin(a.lon) +
//     B * Math.cos(b.lat) * Math.sin(b.lon);
//   var Z = A * Math.sin(a.lat) + B * Math.sin(b.lat);
//   return {
//     lat: Math.atan2(Z, Math.sqrt(Math.pow(X, 2) + Math.pow(Y, 2))),
//     lon: Math.atan2(Y, X),
//   };
// }
// function lineDistance(linestring) {
//   return linestring.reduce(function (d, v, i) {
//     if (i === linestring.length - 1) {
//       return d;
//     }
//     return d + distance(linestring[i], linestring[i + 1]);
//   }, 0);
// }

// function pointOnEdge(v, u, p) {
//   var lon_scale = Math.cos(p[1] * (Math.PI / 180));

//   var bx = v[0] - u[0];
//   var by = v[1] - u[1];

//   var bx2 = bx * lon_scale;
//   var sq = bx2 * bx2 + by * by;

//   var scale =
//     sq > 0 ? ((p[0] - u[0]) * lon_scale * bx2 + (p[1] - u[1]) * by) / sq : 0.0;

//   if (scale <= 0.0) {
//     bx = u[0];
//     by = u[1];
//   } else if (scale >= 1.0) {
//     bx = v[0];
//     by = v[1];
//   } else {
//     bx = bx * scale + u[0];
//     by = by * scale + u[1];
//   }

//   return [bx, by];
// }

// /**
//   project point p on to closest edge of linestring
//   ◯ - - x - - - - ◯ - - - ◯ - - - ◯
//         ┋
//         ◯ P
// **/
// function pointOnLine(linestring, p) {
//   // shortest distance found
//   var d = Infinity;

//   // point to return
//   var r;

//   // edge the projection was performed on
//   var e;

//   for (var x = 0; x < linestring.length - 1; x++) {
//     var a = linestring[x];
//     var b = linestring[x + 1];

//     // project point on to edge A-B
//     var pp = pointOnEdge(a, b, p);

//     // calculate the distance between proj and p
//     var dist = distance(pp, p);

//     // select the projection with the shortest distance from p
//     if (dist < d) {
//       d = dist;
//       r = pp;
//       e = [a, b];
//     }
//   }

//   // return the projected point and the matching edge
//   return { point: r, edge: e, dist: d };
// }

// /**
//   Calculate the distance between two points (in degrees)
//   ◯ < - - - > ◯ ?
// **/

// function distance(a, b) {
//   return toDeg(
//     distance2(
//       { lon: toRad(a[0]), lat: toRad(a[1]) },
//       { lon: toRad(b[0]), lat: toRad(b[1]) }
//     )
//   );
// }

// /*
//   sort coordinate array so the two extremes are the first and last element,
//   the rest of the array is ordered by distance from those points.
//   input: [{ lat: 0.0, lon: 0.0 }, ...]
//   output: same format
// */
// function sort(coords) {
//   switch (coords.length) {
//     case 0:
//       return coords;
//     case 1:
//       return coords;
//     case 2:
//       return coords;
//     default:
//       var maxDist = 0,
//         end = coords[0];

//       // find two extremes (points fathest apart from each other)
//       for (var x = 0; x < coords.length; x++) {
//         for (var y = x + 1; y < coords.length; y++) {
//           var d = distance(
//             [coords[x].lon, coords[x].lat],
//             [coords[y].lon, coords[y].lat]
//           );
//           if (d > maxDist) {
//             maxDist = d;
//             end = coords[x];
//           }
//         }
//       }

//       // calculate distances from p1
//       var sorted = coords.map(function (coord) {
//         coord.dist = distance([end.lon, end.lat], [coord.lon, coord.lat]);
//         return coord;
//       });

//       // sort distances ascending
//       sorted.sort(function (a, b) {
//         return a.dist - b.dist;
//       });

//       return sorted;
//   }
// }

// /*
//   compute the bounding box of an array of coordinates
//   input: [{ lat: 0.0, lon: 0.0 }, ...]
//   output: { lat: { min: 0.0, max: 0.0 }, lon: { min: 0.0, max: 0.0 } }
// */
// function bbox(coords) {
//   return coords.reduce(
//     function (memo, c) {
//       if (c.lat > memo.lat.max) {
//         memo.lat.max = c.lat;
//       }
//       if (c.lat < memo.lat.min) {
//         memo.lat.min = c.lat;
//       }
//       if (c.lon > memo.lon.max) {
//         memo.lon.max = c.lon;
//       }
//       if (c.lon < memo.lon.min) {
//         memo.lon.min = c.lon;
//       }
//       return memo;
//     },
//     {
//       lat: { min: +Infinity, max: -Infinity },
//       lon: { min: +Infinity, max: -Infinity },
//     }
//   );
// }

// /**
//   Calculate the distance (in degrees) of linestring
//   ◯ < - ◯ - - ◯ - - ◯ - > ◯ m?
// **/
// function lineDistance(linestring) {
//   return linestring.reduce(function (d, v, i) {
//     if (i === linestring.length - 1) {
//       return d;
//     }
//     return d + distance(linestring[i], linestring[i + 1]);
//   }, 0);
// }

// /**
//   Copy linestring points until projection, then add the projected point and
//   discard all other line edges from matched edge onwards.
//   in:  ◯ - - ◯ - - ◯ - - ◯
//   out: ◯ - - ◯ - P
// **/
// function sliceLineAtProjection(linestring, proj) {
//   var ret = [];
//   for (var x = 0; x < linestring.length; x++) {
//     var corner = linestring[x];
//     if (corner[0] === proj.edge[1][0] && corner[1] === proj.edge[1][1]) {
//       ret.push(proj.point);
//       return ret;
//     }
//     ret.push(corner);
//   }
//   return ret;
// }

// /**
//   Compute the left/right parity of the projected point relative to the line direction.
//   in:  output of pointOnLine()
//   out: either 'L' or 'R'.. or null in the case where no true answer exists (on the line).
//   @see: https://www.cs.cmu.edu/~quake/robust.html
// **/
// function parity(proj, point) {
//   // validate inputs
//   if (!proj || !proj.edge || !point) {
//     return null;
//   }

//   var acx = proj.edge[0][0] - point[0];
//   var bcx = proj.edge[1][0] - point[0];
//   var acy = proj.edge[0][1] - point[1];
//   var bcy = proj.edge[1][1] - point[1];
//   var xprod = acx * bcy - acy * bcx;

//   // xprod is 0 on the line <0 on the right and >0 on the left
//   if (xprod === 0) {
//     return null;
//   }
//   return xprod < 0 ? "R" : "L";
// }

// function bearing(p1, p2) {
//   var lon1 = toRad(p1[0]),
//     lon2 = toRad(p2[0]);
//   var lat1 = toRad(p1[1]),
//     lat2 = toRad(p2[1]);
//   var a = Math.sin(lon2 - lon1) * Math.cos(lat2);
//   var b =
//     Math.cos(lat1) * Math.sin(lat2) -
//     Math.sin(lat1) * Math.cos(lat2) * Math.cos(lon2 - lon1);
//   return toDeg(Math.atan2(a, b));
// }

// // deduplicate an array or coordinates in geojson [ [ lon, lat ] ... ] format.
function dedupe(coordinates) {
  return coordinates.filter(function (coord, i) {
    if (0 === i) {
      return true;
    }
    if (coord[0] !== coordinates[i - 1][0]) {
      return true;
    }
    if (coord[1] !== coordinates[i - 1][1]) {
      return true;
    }
    return false;
  });
}

// function toRad(degree) {
//   return (degree * Math.PI) / 180;
// }
// function toDeg(radian) {
//   return (radian * 180) / Math.PI;
// }

// /**
//   distance between point A and point B (in radians)
// **/
// function distance(a, b) {
//   return Math.acos(
//     Math.sin(a.lat) * Math.sin(b.lat) +
//       Math.cos(a.lat) * Math.cos(b.lat) * Math.cos(a.lon - b.lon)
//   );
// }

// /**
//   distance between point A and point B (in radians)
//   note: for very short distances this version is less susceptible to rounding error
// **/
// function distance2(a, b) {
//   return (
//     2 *
//     Math.asin(
//       Math.sqrt(
//         Math.pow(Math.sin((a.lat - b.lat) / 2), 2) +
//           Math.cos(a.lat) *
//             Math.cos(b.lat) *
//             Math.pow(Math.sin((a.lon - b.lon) / 2), 2)
//       )
//     )
//   );
// }

// /**
//   course from point A and point B (in radians)
// **/
// function course(a, b, d) {
//   return Math.acos(
//     (Math.sin(b.lat) - Math.sin(a.lat) * Math.cos(d)) /
//       (Math.sin(d) * Math.cos(a.lat))
//   );
// }

// /**
//   cross track error (distance off course)
//   (positive XTD means right of course, negative means left)
// **/
// function crossTrack(d, crs1, crs2, A, B, D) {
//   var calc = crs1 - crs2;

//   // north pole / south pole
//   if (A.lat === +90) {
//     calc = D.lon - B.lon;
//   }
//   if (A.lat === -90) {
//     calc = B.lon - D.lon;
//   }

//   return Math.asin(Math.sin(d) * Math.sin(calc));
// }

// /**
//   along track distance (the distance from A along the course towards B to the point abeam D)
// **/
// function alongTrack(d, xtd) {
//   return Math.acos(Math.cos(d) / Math.cos(xtd));
// }

// /**
//   along track distance (the distance from A along the course towards B to the point abeam D)
//   note: for very short distances this version is less susceptible to rounding error
// **/
// function alongTrack2(d, xtd) {
//   return Math.asin(
//     Math.sqrt(Math.pow(Math.sin(d), 2) - Math.pow(Math.sin(xtd), 2)) /
//       Math.cos(xtd)
//   );
// }

// /**
//   interpolate f (percent) of the distance d along path A-B
// **/
// function interpolate(d, f, a, b) {
//   var A = Math.sin((1 - f) * d) / Math.sin(d);
//   var B = Math.sin(f * d) / Math.sin(d);
//   var X =
//     A * Math.cos(a.lat) * Math.cos(a.lon) +
//     B * Math.cos(b.lat) * Math.cos(b.lon);
//   var Y =
//     A * Math.cos(a.lat) * Math.sin(a.lon) +
//     B * Math.cos(b.lat) * Math.sin(b.lon);
//   var Z = A * Math.sin(a.lat) + B * Math.sin(b.lat);
//   return {
//     lat: Math.atan2(Z, Math.sqrt(Math.pow(X, 2) + Math.pow(Y, 2))),
//     lon: Math.atan2(Y, X),
//   };
// }

// /**
//   calculate the distance a using b and C
//        c
//   A -------B
//    \       |
//     \      |
//      \b    |a
//       \    |
//        \   |
//         \  |
//          \C|
//           \|
//   Napier's rules: tan(a) = tan(b)*cos(C)
// **/
// function project(b, C) {
//   return Math.atan(Math.tan(b) * Math.cos(C));
// }
