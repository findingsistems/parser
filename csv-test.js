var fs = require("fs"),
    http = require("http"),
    unzip = require("unzip"),
    Transform = require('stream').Transform,
    JSFtp = require("jsftp"),
    async = require("async"),
    csv = require('csv-streamify-mod');
var csv_opts = {
    delimiter: ',',
    newline: '\n',
    quote : '\"',
    objectMode: true
};
var transform_cb = function(data, encoding, done) {
    // id | code | relevant_code | manufacturer | name | count | price | price_files_id | user_id | fts | delivere
    if ((!data[2] || data[2].length > 300) || (!data[3] || data[3].length > 150) || (!data[4] || data[4].length > 150) || (!data[5] || data[5].length > 150)) {
        console.log('###ALERT###');
        console.log(data);
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

var csvToJson = csv(csv_opts); //todo make better
var parser = new Transform({objectMode: true}); //todo make better
parser._transform = transform_cb;
var out = fs.createWriteStream("temp/test-1.csv");

fs.createReadStream('temp/' + "autogiper_all_15.csv")
    .pipe(csvToJson)
    .pipe(parser)
    .pipe(out)
    .on('finish', function () {
        console.log('#finish', new Date());
    })
    .on('error', function (err) {
        console.log('error', err);
    });