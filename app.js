process.on('uncaughtException', function (err) {
    console.error(err);
    console.error(err.stack);
});

var fs = require("fs"),
    http = require('http'),
    iconv = require('iconv-lite'),
    parse = require('csv-parse'),
    jschardet = require("jschardet"),
    request = require('request'),
    unzip = require("unzip"),
    Transform = require('stream').Transform,
    cheerio = require('cheerio'),
    JSFtp = require("jsftp"),
    async = require("async"),
    pg = require('pg'),
    copyFrom = require('pg-copy-streams').from,
    csv = require('fast-csv-mod');

var config = {};
var read_config = function () {
    config = {};
    try {
        config = fs.readFileSync('config.json').toString();
        config = JSON.parse(config);
    } catch (e) {
        console.error("Not exist or bad  config.json!");
        throw e;
    };
    //todo check config structure
};
read_config();

var client = new pg.Client(config.db_connection_string); //todo check loss connection
//todo maybe close, and reopen on cycle
client.connect();

if (!fs.existsSync(config.temp_folder))
    fs.mkdirSync(config.temp_folder);
//var task_processed = function () {
//    var task = config.list.shift();
//    console.log("processed task", task.name, "|", new Date());
//    switch (task.type) {
//        case "http":
//            http_processed(task);
//            break;
//        case "ftp":
//            console.log("ftp");
//            break;
//        default:
//            console.error("Unspecified type:", task.type, "| Task:", task.name);
//    }
//};
//
//var http_processed = function (task) {
//    var regexp_file_url, regexp_str;
//    var i = 0;
//    var result_list = [];
//    var url = task.host + task.path;
//
//    if (task.file_extension.length) {
//        regexp_str = "(?:[^\\/][\\d\\w\\.]+)+(";
//        for (i = 0; i < task.file_extension.length; i++)
//            regexp_str += (i === 0 ? task.file_extension[i] : "|" + task.file_extension[i]);
//        regexp_str += ")+$";
//        regexp_file_url = new RegExp(regexp_str, 'gi');
//    }
//    request(url, function (error, response, html) {
//        if (!error && response.statusCode == 200) {
//            var $ = cheerio.load(html);
//            $(task.file_selector).each(function(i, element){
//                //console.log($(element).text());
//                result_list.push($(element).attr('href'));
//            });
//            console.log("Request result:", result_list.length);
//            result_list = result_processed(result_list, regexp_file_url);
//            result_list = file_name_processed(result_list, task.host);
//            download_file(result_list, function(file_name, is_last){
//                console.log('downloaded', file_name, is_last);
//            });
//        } else {
//            console.error(error, response.statusCode);
//        }
//    });
//};
//
//var result_processed = function (array, regexp_file_url) {
//    var f, j, len, ret, v;
//    ret = [];
//    for (j = 0, len = array.length; j < len; j++) {
//        v = array[j];
//        f = regexp_file_url.exec(v);
//        if (f !== null ? f.length : void 0) {
//            ret.push({
//                href: v,
//                file_name: f[0]
//            });
//        }
//    }
//    //todo add file_list check
//    return ret;
//};
//
//var file_name_processed = function (array, host) {
//    var j, len, ret, v;
//    ret = [];
//    for (j = 0, len = array.length; j < len; j++) { //todo add file_mask check
//        v = array[j];
//        if (v.href[0] === "/") {
//            v.href = host + v.href;
//        }
//        ret.push(v);
//    }
//    return ret;
//};
//
//var download_file = function (file_url_list, cb) {
//    var v;
//    var len = file_url_list.length;
//    var count = 0;
//    var transmit_count = 0;
//    var out_file_list = [];
//    console.log("Start interval", file_url_list.length);
//    var inv = setInterval(function(){
//        if (!file_url_list.length)
//           return clearInterval(inv);
//        if (transmit_count >= config.max_file_transmission)
//            return;
//        v = file_url_list.shift();
//        (function (v) {
//            var i = transmit_count++;
//            out_file_list[i] = fs.createWriteStream("./temp/" + v.file_name);
//            http.get(v.href, function (res) {
//                count++;
//                transmit_count--;
//                res.pipe(out_file_list[i]);
//                if (count !== len)
//                    cb(v.file_name, false);
//                else
//                    cb(v.file_name, true);
//            }).on('error', function (e) {
//                count++;
//                transmit_count--;
//                console.error("Get error:", count, v.file_name, e.message);
//                if (count === len)
//                    cb(null, true);
//            });
//        })(v);
//    },500);
//};

var user_id = 1575,
    price_files_id = 0,
    parse_cycle_active = false;

var ftp = new JSFtp({
    host: "ftp.parttrade.ru",
    user: "auto.ru",
    pass: "auto.ru"
});

var csv_opts = {
    headers:    false,
    delimiter:  ',',
    quote :     '\"',
    escape:     '\\',
    objectMode: true
};

var transform_cb = function(data, encoding, done) {
    if ((!data[2] || data[2].length > 300) || (!data[3] || data[3].length > 150) || (!data[4] || data[4].length > 150) || (!data[5] || data[5].length > 150)) {
        console.log('###ALERT###');
        console.log(data);
    }
    var t = "\",\"";
    if (isNaN(+data[3]))
        return done();
    var manufacturer= data[0];
    var code        = data[1].replace(/[^a-zA-Z0-9]/gi, "");
    var name        = data[2];
    var count       = ~~data[3];
    var price       = (~~data[4]);
    var delivere    = (data[5] || "").replace(/[^0-9\-]/gi, "");
    var str = "\""+manufacturer+t+code+t+name+t+count+t+price+t+delivere+t+price_files_id+t+user_id+"\"";
    this.push(str+"\n");
    done();
};
var query = 'COPY prices_wholesale (manufacturer, code, name, count, price, delivere, price_files__id, user__id) FROM STDIN CSV';

var processed_file = async.queue(function (task, callback) {
    console.log('each preprocessed', task.name);
    var csvToJson = csv(csv_opts); //todo make better
    var parser = new Transform({objectMode: true}); //todo make better
    parser._transform = transform_cb;
    var stream_db = client.query(copyFrom(query));
    stream_db.on("error", function(err) {
        console.log("#ERROR stream_db", err)
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
            console.log('#ERROR task.entry', err);
            callback();
            if (preprocessed_list.length)
                preprocessed_file();
            else
                preprocessed_active = false;
        });
}, 1);
processed_file.drain = function() {
    parse_cycle_active = false;
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
        fs.createReadStream(config.temp_folder + file_name)
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

var parse_cycle = function() {
    console.log("PARSE CYCLE", new Date());
    var to_price_files = [
        user_id,
        "ftp.parttrade.ru/autogiper_all_csv.zip",
        "autogiper_all_csv.zip",
        "Обработка завершена",
        1,
        {"goods_quality":"1","delivery_time":"1","discount":"0"},
        3
    ];
    client.query('INSERT INTO price_files (user__id, path, name, status, active, info, price_type__id) VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id', to_price_files, function(err, insert_result) {
        if (err) return console.error(err);

        console.log('price_files_id', insert_result.rows[0].id)
        if (insert_result.rows[0].id)
            price_files_id = insert_result.rows[0].id;
        else
            return console.log("### ERROR on get price_files_id");

        client.query('DELETE FROM prices_wholesale WHERE user__id=$1', [user_id], function(err) {
            if (err) return console.error(err);

            console.log("Cleared prev rows user_id="+user_id, new Date());
            ftp.ls(".", function (err, res) {
                async.eachLimit(res, 1, function (file, cb) {
                    if (file.name !== "autogiper_all.zip")
                        return cb();

                    console.log("start download", file.name);
                    ftp.get("./" + file.name, config.temp_folder + file.name, function (err) {
                        if (err) return cb(err);
                        console.log('downloaded', config.temp_folder + file.name);
                        preprocessed_file(file.name);
                        cb();
                    });
                }, function (err) {
                    if (err) return console.log(err);
                    console.log('all files were downloaded');
                });
            });
        });
    });
};

parse_cycle();

var parse_intv = setInterval(function(){
    if (parse_cycle_active) return console.error("Not finished prev cycle!");

    parse_cycle_active = true; //todo on error set false
    parse_cycle();
}, 864e5);

/*
- each on config.list, use async
  -- connect to file storage
  -- get file list and filtering from config
  -- check file changed
  -- insert to price_files and delete old rows on table to insert(prices_wholesale)
  -- processing file(unzip if archive), check file type from config
  -- transform from config options
- close all connection


# DB structure
id | code | relevant_code | manufacturer | name | count | price | price_files_id | user_id | fts | delivere
 */