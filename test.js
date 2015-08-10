var fs = require("fs"),
    http = require("http"),
    unzip = require("unzip"),
    Transform = require('stream').Transform,
    JSFtp = require("jsftp"),
    async = require("async"),
    pg = require('pg'),
    copyFrom = require('pg-copy-streams').from,
    csv = require('csv-streamify-mod');

var conString = "postgres://autogiper:autogiper@localhost/autogiper";
var client = new pg.Client(conString);
client.connect();

var ftp = new JSFtp({
    host: "ftp.parttrade.ru",
    user: "auto.ru",
    pass: "auto.ru"
});

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
    var code        = data[1]; //todo leave only numbers and symbols
    var name        = data[2];
    var count       = ~~data[3];
    var price       = (~~data[4]);
    var delivere    = data[5] || ""; //todo leave only numbers and "-"
    var str = "\""+manufacturer+t+code+t+name+t+count+t+price+t+delivere+"\"";
    this.push(str+"\n");
    //this.push(data.join(";")+"\n");
    done();
};
var query = 'COPY prices_parser (manufacturer, code, name, count, price, delivere) FROM STDIN CSV';

var processed_file = async.queue(function (task, callback) {
    console.log('each preprocessed', task.name);
    var csvToJson = csv(csv_opts); //todo make better
    var parser = new Transform({objectMode: true}); //todo make better
    parser._transform = transform_cb;
    var stream_db = client.query(copyFrom(query));
    stream_db.on("error", function(err) {
        console.log("#ERROR", err)
    });
    stream_db.on("end", function() {
        console.log("###END##");
        callback(); //todo check call task.entry.error
        if (preprocessed_list.length)
            preprocessed_file();
        else
            preprocessed_active = false;
    });
    task.entry
        .pipe(csvToJson)
        .pipe(parser)
        .pipe(stream_db)
        .on('finish', function () {
            console.log('#finish', new Date());
        })
        .on('error', function (err) {
            console.log('error', err);
            callback();
            if (preprocessed_list.length)
                preprocessed_file();
            else
                preprocessed_active = false;
        });
}, 1);
processed_file.drain = function() {
    console.log('all items have been processed');
};

var preprocessed_active = false;
var preprocessed_list = [];
var preprocessed_file = function(file_name_new) {
    if (preprocessed_active && file_name_new !== undefined) {
        preprocessed_list.push(file_name_new);
    } else {
        if (file_name_new !== undefined)
            preprocessed_list.push(file_name_new);
        if (preprocessed_list.length === 0)
            return;
        preprocessed_active = true;
        var file_name = preprocessed_list.shift();
        fs.createReadStream('temp/' + file_name)
            .pipe(unzip.Parse())
            .on('entry', function (entry) {
                var fileName = entry.path;
                var type = entry.type; // 'Directory' or 'File'
                console.log('ZIP files',fileName, type);
                if (/\.(csv)$/i.test(fileName) && type === "File") { //todo check Directory
                    processed_file.push({name: fileName, entry: entry});
                } else {
                    entry.autodrain();
                }
            })
            .on('close', function () {
                console.log('END 1', new Date());
            });
    }
};

ftp.ls(".", function(err, res) {
    async.eachLimit(res, 1, function(file, cb) {
        //if (file.name != "autogiper_stock.zip" && file.name != "zzap_stock.zip")
        if (file.name != "autogiper_all_csv.zip")
            return cb();

        console.log("start download", file.name);
        ftp.get("./"+file.name, "./temp/"+file.name, function(err) {
            if (err) return cb(err);
            console.log('downloaded', "./temp/"+file.name);
            preprocessed_file(file.name);
            cb();
        });
    }, function(err) {
        if (err) return console.log(err);
        console.log('all files were downloaded');
    });
});