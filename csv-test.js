var fs = require("fs"),
    http = require("http"),
    unzip = require("unzip"),
    Transform = require('stream').Transform,
    JSFtp = require("jsftp"),
    async = require("async"),
    // csv = require('fast-csv-mod');
    // csv = require('csv-streamify-mod');
    csv = require('csv-parser');
// var csv_opts = {
//     headers:    false, 
//     delimiter:  ',',
//     quote :     '\"',
//     escape:     '\\',
//     objectMode: true
// };
// var csv_opts = {
//    delimiter: ',',
//    newline: '\n',
//    quote : '\"',
//    objectMode: true
// };
var csv_opts = {
    raw: false,
    separator: ',',
    newline: '\n',
    headers: ['0', '1', '2', '3', '4', '5'],
    objectMode: true
};
var transform_cb = function(data, encoding, done) {
    // id | code | relevant_code | manufacturer | name | count | price | price_files_id | user_id | fts | delivere
    if ((!data[2] || data[2].length > 300) || (!data[3] || data[3].length > 150) || (!data[4] || data[4].length > 150) || (!data[5] || data[5].length > 150)) {
       console.log('###ALERT###');
       // console.log(data);
    }
    var t = "\",\"";
    if (isNaN(+data[3]))
        return done();
    var manufacturer= data[0];
    var code        = data[1];
    var name        = data[2];
    var count       = ~~data[3];
    var price       = (~~data[4]);
    var delivere    = data[5] || ""; //todo leave only numbers and "-"
    var str = "\""+manufacturer+t+code+t+name+t+count+t+price+t+delivere+"\"";
    this.push(str+"\n");
    //this.push(data.join(";")+"\n");
    done();
};

var ts_1 = 0,
    ts_2 = 0,
    ts_3 = 0,
    ts_4 = 0,
    ts_5 = 0,
    c_o  = 0,
    c_2  = 0,
    c_3  = 0,
    tick = 100000;
var out = fs.createWriteStream("temp/test-1.csv");
var parser = new Transform({objectMode: true}); //todo make better
parser._transform = transform_cb;
parser.on("data", function(data){
    if (c_3%tick === 0) {
        if (c_3 > 0)
            console.log('csvToJson', ~~(c_3/tick), Date.now() - ts_3);
    }
    c_3++;
    out.write(data);
})
.on('end', function () {
    console.log("parser end",  ~~((Date.now() - ts_1)/1000));
    out.end();
});
var csvToJson = csv(csv_opts); //todo make better
csvToJson.on("data", function(data){
    if (c_2%tick === 0) {
        ts_3 = Date.now();
        if (c_2 > 0) {
            console.log('csvToJson', ~~(c_2/tick), Date.now() - ts_2 - ts_4);
            ts_4 = Date.now() - ts_2;
        }
    }
    c_2++;
    parser.write(data);
})
.on('end', function () {
    console.log("csvToJson end");
    parser.end();
});

console.log('#start');
ts_1 = Date.now();
fs.createReadStream('temp/' + "autogiper_all_15.csv")
    .on('data', function(chunk){
        if (c_o%tick === 0) {
            console.log('read '+tick)
            ts_2 = Date.now();
        }
        c_o++;
        csvToJson.write(chunk);
    })
    .on('end', function () {
        csvToJson.end();
    })
    .on('error', function (err) {
        console.log('error ReadStream', err);
    });
out.on('finish', function(){
    console.log('#finish out');
});
//fs.createReadStream('temp/' + "autogiper_all_15.csv")
//    .pipe(csvToJson)
//    .pipe(parser)
//    .pipe(out)
//    .on('finish', function () {
//        console.log('#finish', ~~((Date.now() - ts_1)/1000), new Date());
//    })
//    .on('error', function (err) {
//        console.log('error', err);
//    });