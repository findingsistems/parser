"use strict";
process.on('uncaughtException', function (err) {
  console.error(err);
  console.error(err.stack);
});

var fs = require("fs"),
  pg = require('pg'),
  csv = require('fast-csv-mod'),
  http = require('http'),
  unzip = require("unzip"),
  //iconv = require('iconv-lite'),
  spawn = require('child_process').spawn,
  JSFtp = require("jsftp"),
  async = require("async"),
  request = require('request'),
  cheerio = require('cheerio'),
  copyFrom = require('pg-copy-streams').from,
  //jschardet = require("jschardet"),
  Transform = require('stream').Transform;

var config, db_client, parse_intv,
  parse_cycle_active = false;

var read_config = function () {
  config = {};
  try {
    config = fs.readFileSync('config.json').toString();
    config = JSON.parse(config);
  } catch (e) {
    console.error("Not exist or bad  config.json!");
    throw e;
  }
  //todo check config structure
};
read_config();

/*
- default value
- cancel if empty || NaN
- regexp
- function
-- toInt
 */
var cancel_check = function( type, data, done ) {
  var check = true;
  switch ( type ){
    case "NaN" :
      check = isNaN( data );
      break;
    case "length" :
      check = data != null && !data.length;
      break;
    case "null" :
    case "undefined" :
      check = data != null && data !== undefined;
      break;
    default :
      console.error( "Cancel check", type );
  }
  if ( check ){
    return done();
  } else {
    return data;
  }
};

var transform_column = function( column_opt ) {
  var r;
  switch ( column_opt.type ) {
    case "regexp" :
      r = new RegExp( column_opt.value, column_opt.options );
      return function( data, done ){
        var ret = data[column_opt.column].replace( r, "" );
        if ( column_opt.cancel != null ) {
          return cancel_check( column_opt.cancel, ret, done );
        } else {
          return ret;
        }
      };
      break;
    case "number_latter":
      return function( data, done ){
        var ret = data[column_opt.column].replace( /[^a-zA-Z0-9]/gi, "" );
        if ( column_opt.cancel != null ) {
          return cancel_check( column_opt.cancel, ret, done );
        } else {
          return ret;
        }
      };
      break;
    case "to_int":
      return function( data, done ){
        var ret = ~~data[column_opt.column];
        if ( column_opt.cancel != null ) {
          return cancel_check( column_opt.cancel, ret, done );
        } else {
          return ret;
        }
      };
      break;
    default :
      if ( !column_opt.type ) {
        return function( data ){
          return data[column_opt.column];
        }
      } else {
        console.error( "Not defined type", column_opt.type);
        return function( data ){
          return data[column_opt.column];
        }
      }
  }

};

var transform_compiler = function( task ) {
  var manufacturer = transform_column( task.transform_opts.manufacturer );
  var code = transform_column( task.transform_opts.code );
  var name = transform_column( task.transform_opts.name );
  var count = transform_column( task.transform_opts.count );
  var price = transform_column( task.transform_opts.price );
  var delivere = transform_column( task.transform_opts.delivere );

  return function (data, encoding, done) {
    if ( (!data[2] || data[2].length > 300) || (!data[3] || data[3].length > 150) || (!data[4] || data[4].length > 150) || (!data[5] || data[5].length > 150) ) {
      console.log('###ALERT###');
      console.log(data);
    }
    var t = "\",\"";
    var str = "\"" + manufacturer( data, done ) + t ;
    str += code( data, done ) + t;
    str += name( data, done ) + t;
    str += count( data, done ) + t;
    str += price( data, done ) + t;
    str += delivere( data, done ) + t;
    str += this._price_files_id + t + this._user_id + "\"";
    this.push(str + "\n");
    done();
  };
};

var query = 'COPY prices_wholesale (manufacturer, code, name, count, price, delivere, price_files__id, user__id) FROM STDIN CSV';

/*
 * PROCESSED FILE
 */
var processed_file = async.queue(function (obj, callback) { //todo make better
  var toJSON, parser, stream_db;
  if ( /\.(csv)$/i.test( obj.file_name ) ) {
    toJSON = csv(obj.task.csv_opts);
  } else {
    console.log("todo add format", obj.file_name);
    return callback();
    //toJSON= csv(obj.task.xls_opts);
  }

  parser = new Transform({objectMode: true});
  parser._transform = transform_compiler( obj.task );
  parser._task = obj.task;
  parser._user_id = obj.task.user_id;
  parser._price_files_id = obj.price_files_id;

  stream_db = db_client.query(copyFrom(query));
  //stream_db = fs.createWriteStream('temp/out-test.csv');
  stream_db.on("error", function (err) {
    console.log("# ERROR stream_db", err);
  });
  stream_db.on("end", function () {
    callback(); //todo check call task.entry.error
  });

  obj.entry
    .pipe(toJSON)
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


/*
 * PREPROCESSED FILE
 */
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
     if ( err ) return cb( err );
     if ( !res.rows[0].id ) return cb("### ERROR on get price_files_id");

     db_client.query('DELETE FROM prices_wholesale WHERE user__id=$1', [user_id], function (err) {
       if ( err ) return cb( err );
       cb( err, res.rows[0].id );
     });
   });
  //cb(null, 1234);
};

var preprocessed_file = async.queue(function ( obj, callback ) {
  var r_get_extension = /\.(.*)$/i;
  console.log('preprocessed_file', obj.file_name); 
  if ( /\.(zip)$/i.test( obj.file_name ) ) {
    fs.createReadStream( config.temp_folder + "/" + obj.file_name )
      .pipe( unzip.Parse() )
      .on( 'entry', function ( entry ) {
        var file_name = entry.path,
          path;
        //var type = entry.type; // 'Directory' or 'File'
        if ( ~obj.task.file_extension_to_processed.indexOf( r_get_extension.exec( file_name )[1] ) ) { //todo check Directory
          obj.entry = entry;
          obj.file_name = file_name;
          path = obj.task.host + "/" + obj.task.path + "/" + file_name;
          db_preparation( obj.task.user_id, path, file_name, function(err, price_files_id){
            if ( err ) return console.log( err );
            console.log(file_name, 'price_files_id', price_files_id);
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
      obj.entry = fs.createReadStream( config.temp_folder + "/" + obj.file_name );
      var path = obj.task.host + "/" + obj.task.path + "/" + obj.file_name;
      db_preparation( task.user_id, path, obj.file_name, function(err, price_files_id){
        if ( err ) return console.log( err );
        console.log('price_files_id', obj.file_name, price_files_id);
        obj.price_files_id = price_files_id;
        processed_file.push( obj );
      });
    }
    callback();
  }
}, 1);

/*
 * SWITCH TASK and TASK TYPE PROCESSED
 */

var check_file_to_download = function( task, file_name ){
  if ( ~task.file_list.indexOf( file_name ) ) return true;
  //todo check task.file_mask and task.file_extension_to_download
  return false;
};

var ftp_processed = function( task ) {
  var ftp = new JSFtp({
    host: task.host,
    user: task.user,
    pass: task.pass
  });

  ftp.ls( task.path, function (err, res) {
    async.eachLimit( res, 1, function ( file, cb ) {
      if ( !check_file_to_download( task, file.name ) ) return cb();
      console.log("start download", file.name);

      ftp.get( task.path + "/" + file.name, config.temp_folder + "/" + file.name, function ( err ) {
        if ( err ) return cb( err );
        console.log('downloaded', config.temp_folder + "/" + file.name);

        preprocessed_file.push({task: task, file_name: file.name});
        cb();
      });
    }, function ( err ) {
      if ( err ) return console.log( err );
      console.log('all files were downloaded'); //todo add cb
    });
  });
};


var http_processed = function (task) {
  var result_list = [],
    url = task.host + task.path;

  request(url, function (error, response, html) {
    if (!error && response.statusCode == 200) {
      var $ = cheerio.load(html);

      $(task.file_selector).each(function(i, element){
        //console.log($(element).text());
        result_list.push({
          name: $(element).text(),
          href: $(element).attr('href')
        });
      });

      async.eachLimit( result_list, 1, function ( file, cb ) {
        if ( !check_file_to_download( task, file.name ) ) return cb();
        console.log("start download", file.name);

        http.get( file.href, function ( res ) {
          var out = fs.createWriteStream( config.temp_folder + "/" + file.name );
          out
            .on( "error", cb )
            .on("finish", function(){
              preprocessed_file.push({task: task, file_name: file.name});
              cb();
            });

          res.pipe(out);
        }).on( 'error', cb );
      }, function ( err ) {
        if ( err ) return console.log( err );
        console.log('all files were downloaded'); //todo add cb
      });

    } else {
      console.error(error, response.statusCode); //todo add cb
    }
  });
};

var task_processed = function(task, cb) {
  switch (task.type) {
    case "ftp":
      ftp_processed(task);
      break;
    case "http":
      http_processed(task);
      break;
    default :
      console.log('http not set');
      break;
  }
  (function(){
    processed_file.drain = function () {
      if ( preprocessed_file.idle() ){
        console.log('###all items have been processed', task.name);
        cb();
      }
    };
  })(task, cb);
};

/*
 * MAIN INTERVAL
 */
var parse_cycle = function () {
  var parse_check_interval = config.check_interval;
  if ( parse_cycle_active ) return console.error( "Not finished prev cycle!" );
  console.log('# Start parse cycle', new Date());

  parse_cycle_active = true; //todo on error set false
  read_config();
  if ( !parse_intv || parse_check_interval !== config.check_interval ) {
    if ( parse_intv ) clearInterval( parse_intv );
    parse_intv = setInterval( parse_cycle, config.check_interval );
  }

   db_client = new pg.Client( config.db_connection_string );
   db_client.connect();

  if ( !fs.existsSync( config.temp_folder ) )
    fs.mkdirSync( config.temp_folder );

  async.eachLimit( config.list, 1, function ( task, cb ) {
    console.log("start task", task.name, new Date());
    task_processed( task, cb );
  }, function ( err ) {
    if ( err ) return console.log(err);
      console.log('# All task were finished', new Date());
      db_client.end();
      //todo clear temp folder
      //todo close all connection
      spawn('sh', [ 'db_resetxlog.sh' ]);
      parse_cycle_active = false;
  });
};
parse_cycle();

/*
 ,
 {
 "name": "test_1 91.197.10.216",
 "type": "http",
 "host": "http://91.197.10.216",
 "path": "/files/",
 "file_selector": "a[href^='/files/']",
 "file_extension": ["zip", "csv", "xls"],
 "file_list": [],
 "file_mask": [""]
 }

 # DB structure
 id | code | relevant_code | manufacturer | name | count | price | price_files_id | user_id | fts | delivere
 */