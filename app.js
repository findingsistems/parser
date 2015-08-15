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
  }
  ;
  //todo check config structure
};
read_config();

var db_client = null;
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

var parse_cycle_active = false;

var csv_opts = {
  headers: false,
  delimiter: ',',
  quote: '\"',
  escape: '\\',
  objectMode: true
};

var transform_cb = function (data, encoding, done) {
  if ((!data[2] || data[2].length > 300) || (!data[3] || data[3].length > 150) || (!data[4] || data[4].length > 150) || (!data[5] || data[5].length > 150)) {
    console.log('###ALERT###');
    console.log(data);
  }
  var t = "\",\"";
  if (isNaN(+data[3]))
    return done();
  var manufacturer = data[0];
  var code = data[1].replace(/[^a-zA-Z0-9]/gi, "");
  var name = data[2];
  var count = ~~data[3];
  var price = (~~data[4]);
  var delivere = (data[5] || "").replace(/[^0-9\-]/gi, "");
  var str = "\"" + manufacturer + t + code + t + name + t + count + t + price + t + delivere + t + price_files_id + t + user_id + "\""; //fixme GET user_id and price_files_id
  this.push(str + "\n");
  done();
};
var query = 'COPY prices_wholesale (manufacturer, code, name, count, price, delivere, price_files__id, user__id) FROM STDIN CSV';

var processed_file = async.queue(function (obj, callback) {
  var csvToJson = csv(obj.task.csv_opts); //todo make better
  var parser = new Transform({objectMode: true}); //todo make better
  parser._transform = transform_cb;
  var stream_db = db_client.query(copyFrom(query));
  stream_db.on("error", function (err) {
    console.log("# ERROR stream_db", err)
  });
  stream_db.on("end", function () {
    callback(); //todo check call task.entry.error
  });
  obj.entry
    .pipe(csvToJson)
    .pipe(parser)
    .pipe(stream_db)
    .on('finish', function () {
      console.log('finish', new Date());
    })
    .on('error', function (err) {
      console.log('# ERROR task.entry', err);
      callback();
    });
}, 1);
processed_file.drain = function () {
  if ( preprocessed_file.idle() ){
    console.log('###all items have been processed'); //fixme call obj.done_cb()
  }
};

var preprocessed_file = async.queue(function ( obj, callback ) {
  if ( /\.(zip)$/i.test( obj.file_name ) ) {
    fs.createReadStream( config.temp_folder + obj.file_name )
      .pipe( unzip.Parse() )
      .on( 'entry', function ( entry ) {
        var file_name = entry.path,
          path;
        //var type = entry.type; // 'Directory' or 'File'

        if ( ~obj.task.file_extension_to_processed.indexOf( file_name ) ) { //todo check Directory
          obj.entry = entry;
          path = obj.task.host + "/" + obj.task.path + "/" + file_name;
          db_preparation( task.user_id, path, file_name, function(err, price_files_id){
            if ( err ) return console.log(err);
            console.log('price_files_id', file_name, price_files_id);
            obj.price_files_id = price_files_id;
            processed_file.push( obj );
          });
          callback();
        } else {
          entry.autodrain();
        }
      })
      .on( 'error', function () {
        console.log('### ERROR - preprocessed_file', obj);
        callback();
      });
  } else {
    if ( ~obj.task.file_extension_to_processed.indexOf( obj.file_name ) ) {
      obj.entry = fs.createReadStream( config.temp_folder + obj.file_name );
      var path = obj.task.host + "/" + obj.task.path + "/" + obj.file_name;
      db_preparation( task.user_id, path, obj.file_name, function(err, price_files_id){
        if ( err ) return console.log(err);
        console.log('price_files_id', obj.file_name, price_files_id);
        obj.price_files_id = price_files_id;
        processed_file.push( obj );
      });
    }
    callback();
  }
}, 1);

var check_file_to_download = function( task, file_name ){
  if ( ~task.file_list.indexOf( file_name ) ) return true;
  //todo check task.file_mask and task.file_extension_to_download
  return false;
};

var db_preparation = function(user_id, path, file_name, cb) {
  var to_price_files = [
    user_id,
    path + "/" + file_name,
    "" + file_name,
    "Обработка завершена",
    1,
    {"goods_quality": "1", "delivery_time": "1", "discount": "0"},
    3 //todo maybe to config
  ];
  db_client.query('INSERT INTO price_files (user__id, path, name, status, active, info, price_type__id) VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id', to_price_files, function (err, res) {
    if (err) return cb(err);
    if (!res.rows[0].id) return cb("### ERROR on get price_files_id");

    db_client.query('DELETE FROM prices_wholesale WHERE user__id=$1', [user_id], function (err) {
      if (err) return cb(err);
      cb(err, res.rows[0].id);
    });
  });

};

var ftp_processed = function( task, done ) {
  var ftp = new JSFtp({
    host: task.host,
    user: task.user,
    pass: task.pass
  });

  ftp.ls( task.path, function (err, res) {
    async.eachLimit( res, 1, function ( file, cb ) {
      if ( !check_file_to_download( task, file.name ) ) return cb();
      console.log("start download", file.name);

      ftp.get( "./" + file.name, config.temp_folder + file.name, function ( err ) {
        if ( err ) return cb( err );
        console.log('downloaded', config.temp_folder + file.name);

        preprocessed_file.push({task: task, file_name: file.name, done_cb: done});
        cb();
      });
    }, function ( err ) {
      if ( err ) return console.log(err);
      console.log('all files were downloaded');
    });
  });
};

var task_processed = function(task, cb) {
  switch (task.type) {
    case "ftp":
      ftp_processed(task, cb);
      break;
    default :
      console.log('http not set');
      break;
  }
};

var parse_intv = setInterval(function () { //todo check changes config.check_interval
  if (parse_cycle_active) return console.error("Not finished prev cycle!");
  console.log('# Start parse cycle', new Date());

  parse_cycle_active = true; //todo on error set false
  read_config();
  db_client = new pg.Client(config.db_connection_string);
  db_client.connect();

  if (!fs.existsSync(config.temp_folder))
    fs.mkdirSync(config.temp_folder);

  async.eachLimit(config.list, 1, function ( task, cb ) {
    console.log("start task", task.name, new Date());
    task_processed(task, cb);
  }, function ( err ) {
    if ( err ) return console.log(err);
    console.log('# All task were finished', new Date());
    db_client.end();
    parse_cycle_active = false;
  });
}, config.check_interval);

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