/*jshint esversion: 6 */

//step 1
/*
const parser = require('csv').parse({
  columns: true,
  trim: true,
  skip_empty_lines: true
});

const fs = require('fs');
const inputStream = fs.createReadStream('./input.csv', 'utf-8');

parser.on('readable', function(){

  const data = parser.read();
  if(data){
    console.log(JSON.stringify(data));
  }
});

inputStream.pipe(parser);
*/

//step 2
const Promise = require('bluebird');
const mappify = require("mappify").getClient("920284e3-5e81-4a84-8590-51c991bf8868");

const geocode = (streetAddress, postCode, city, state) => {

  return new Promise((resolve, reject) => {

    mappify.geocode(streetAddress, postCode, city, state, (err, res) => {

        if(err) return reject(err);

        return resolve(res);
    });
  });
};


const LineByLineReader = require('line-by-line');
const fs = require('fs');
const lr = new LineByLineReader('./input.json');

const output = fs.createWriteStream('./output.json');
const errors = fs.createWriteStream('./errors.txt');

lr.on('error', function (err) {
  errors.write(JSON.stringify(err) + '\n');
});

lr.on('line', function (line) {

  lr.pause();
  const data = JSON.parse(line);

  if(!Object.keys(data).length) return lr.resume();

  Promise.try(() => geocode(
    data.StreetAddress || null,
    data.Postcode || null,
    data.Suburb || null,
    data.State || null
  ))
  .then((resp) => {

    if(!resp.result){
      errors.write(`Failed line ${data.ID}:  ${line} \n`);
    }

    data.mappify_buildingName = resp.result.buildingName;
    data.mappify_numberFirst = resp.result.numberFirst;
    data.mappify_numberFirstPr = resp.result.numberFirstPr;
    data.mappify_numberFirstSu = resp.result.numberFirstSu;
    data.mappify_numberLast = resp.result.numberLast;
    data.mappify_numberLastPre = resp.result.numberLastPre;
    data.mappify_numberLastSuf = resp.result.numberLastSuf;
    data.mappify_streetName = resp.result.streetName;
    data.mappify_streetType = resp.result.streetType;
    data.mappify_streetSuffixC = resp.result.streetSuffixC;
    data.mappify_suburb = resp.result.suburb;
    data.mappify_state = resp.result.state;
    data.mappify_postCode = resp.result.postCode;
    data.mappify_location = resp.result.location;
    data.mappify_streetAddress = resp.result.streetAddress;

    output.write(JSON.stringify(data) + '\n');

    lr.resume();
  })
  .catch((err) => {
    errors.write(`Failed line ${data.ID}: ${JSON.stringify(err)} \n`);
    lr.resume();
  });

});

lr.on('end', function () {
  process.nextTick(() => process.exit());
});

