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
  iconv = require('iconv-lite'),
  spawn = require('child_process').spawn,
  JSFtp = require("jsftp"),
  async = require("async"),
  request = require('request'),
  cheerio = require('cheerio'),
  copyFrom = require('pg-copy-streams').from,
  //jschardet = require("jschardet"),
  Transform = require('stream').Transform;

var config, db_client, parse_intv, query,
  task_begin = {},
  task_all_downloaded =false,
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

var transform_column = function( column_opt, task ) {
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
    case "delete_quote":
      return function( data, done ){
        var ret = data[column_opt.column].replace( /[\"\']/gi, "" );
        if ( column_opt.cancel != null ) {
          return cancel_check( column_opt.cancel, ret, done );
        } else {
          return ret;
        }
      };
      break;
    case "to_int":
      return function( data, done ){
        var ret = data[column_opt.column ].split( ','  )[0];
        ret = ~~ret;
        if ( column_opt.cancel != null ) {
          return cancel_check( column_opt.cancel, ret, done );
        } else {
          return ret;
        }
      };
      break;
    case "id_to_delivere":
      return function( data, done ){
        var ret = task.id_to_delivere[task.file_id];
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
  var manufacturer = transform_column( task.transform_opts.manufacturer, task );
  var code = transform_column( task.transform_opts.code, task );
  var name = transform_column( task.transform_opts.name, task );
  var count = transform_column( task.transform_opts.count, task );
  var price = transform_column( task.transform_opts.price, task );
  var delivere = transform_column( task.transform_opts.delivere, task );

  return function (data, encoding, done) {
    var d = [], key;
    if ( data !== null && typeof data === 'object' ) {
      for (key in data) {
        d.push(data[key]);
      }
    } else {
      d = data;
    }
    //if ( (!d[2] || d[2].length > 300) || (!d[3] || d[3].length > 150) || (!d[4] || d[4].length > 150) || (!d[5] || d[5].length > 150) ) {
    //  console.log('###ALERT###');
    //  console.log(d);
    //}
    var t = "\",\"";
    var str = "\"" + manufacturer( d, done ) + t ;
    str += code( d, done ) + t;
    str += name( d, done ) + t;
    str += count( d, done ) + t;
    str += price( d, done ) + t;
    str += delivere( d, done ) + t;
    str += this._price_files_id + t + this._user_id + "\"";
    this.push(str + "\n");
    done();
  };
};

query = 'COPY prices_wholesale (manufacturer, code, name, count, price, delivere, price_files__id, user__id) FROM STDIN CSV';

/*
 * PROCESSED FILE
 */
var processed_file = async.queue(function ( obj, callback ) { //todo make better
  var toJSON, parser, stream_db;
  if ( /\.(csv)$/i.test( obj.file_name ) ) {
    toJSON = csv( obj.task.csv_opts );
  } else {
    console.log( "todo add format", obj.file_name );
    return callback();
    //toJSON= csv(obj.task.xls_opts);
  }
  console.log('START processed file', obj.file_name, new Date());

  parser = new Transform({objectMode: true});
  parser._transform = transform_compiler( obj.task );
  parser._task = obj.task;
  parser._user_id = obj.task.user_id;
  parser._price_files_id = obj.price_files_id;

  stream_db = db_client.query(copyFrom(query));
  //stream_db = fs.createWriteStream('temp/out-test-' + obj.task.file_id +'.csv');
  stream_db.on( "error", function ( err ) {
    console.log( "# ERROR stream_db", err );
  });
  stream_db.on( "end", function () {
    callback(); //todo check call task.entry.error
  });

  obj.entry
    .pipe( iconv.decodeStream( obj.task.encoding || "utf8" ) )
    .pipe( toJSON )
    .pipe( parser )
    .pipe( stream_db )
    .on( 'error', function ( err ) {
      console.log( '# ERROR task.entry', err );
      callback();
    });
}, 1);


/*
 * PREPROCESSED FILE
 */
var db_preparation = function(task, path, file_name, cb) {
  var need_drop_rows = false;
  var to_price_files = [
   task.user_id,
   path + "/" + file_name,
   "" + file_name,
   "Обработка завершена",
   1,
   {"goods_quality": "1", "delivery_time": "1", "discount": "0"},
   3
  ];
  task.droped_rows || (need_drop_rows = true);
  task.droped_rows = true;
  db_client.query('INSERT INTO price_files (user__id, path, name, status, active, info, price_type__id) VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id', to_price_files, function (err, res) {
   if ( err ) return cb( err );
   if ( !res.rows[0].id ) return cb("### ERROR on get price_files_id");

   if ( need_drop_rows ) {
     db_client.query( 'DELETE FROM prices_wholesale WHERE user__id=$1', [ task.user_id ], function ( err ) {
       if ( err ) return cb( err );
       cb( err, res.rows[ 0 ].id );
     });
   } else {
     cb( err, res.rows[ 0 ].id );
   }
  });
  //cb(null, 1234);
};

var preprocessed_file = async.queue(function ( obj, callback ) {
  var r_get_extension = /\.(.*)$/i;
  var r_get_file_id = /\[(\d*)\]/g;
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
          if ( obj.task.file_id_check ) { //todo remake
            var id = r_get_file_id.exec( file_name );
            if ( id && id[1] != null) {
              obj.task.file_id = id[1];
            } else {
              return cb( "NOT GET FILE ID" );
            }
          }
          db_preparation( obj.task, path, file_name, function(err, price_files_id){
            if ( err ) return console.log( err );
            obj.price_files_id = price_files_id;
            processed_file.push( obj );
          });
          callback();
        } else {
          entry.autodrain();
        }
      })
      .on( 'error', function () {
        console.log('### ERROR - preprocessed_file', obj.file_name);
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
 * TASK BEGIN
 */
task_begin.preprocessed_file_1 = function( url, task, cb ){
  task.id_to_delivere = {};

  http.get( url, function ( res ) {
    var toJSON, get_opt;
    if ( res.statusCode === 200 ) {
      toJSON = csv( {
        "headers": true,
        "delimiter": ";",
        "objectMode": true
      } );
      get_opt = new Transform( { objectMode: true } );
      get_opt._transform = function ( data, encoding, done ) {
        task.id_to_delivere[ data.postID ] = data.Days + "-" + data.DaysMax;
        done();
      };

      res
        .pipe( toJSON )
        .pipe( get_opt )
        .on( "finish", function () {
          cb();
        } )
        .on( "error", cb );
    } else {
      cb("STATUS CODE != 200");
    }
  }).on( 'error', cb );
};

var task_begin_check = function( task, cb ) {
  if ( task.task_begin && task_begin[task.task_begin.name] != null) {
    task_begin[task.task_begin.name]( task.task_begin.url, task, cb);
  } else {
    cb()
  }
};

/*
 * SWITCH TASK and TASK TYPE PROCESSED
 */
var check_file_to_download = function( task, file_name ){
  var r_get_extension = /\.(.*)$/i;
  if ( ~task.file_list.indexOf( file_name ) ) return true;
  if ( r_get_extension.exec( file_name ) && ~task.file_extension_to_download.indexOf( r_get_extension.exec( file_name )[1] ) ) return true;
  //todo check task.file_mask
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
      task_all_downloaded = true;
      console.log('all files were downloaded'); //todo add cb
    });
  });
};


var http_processed = function (task) {
  var result_list = [],
    url = task.host + '/' + task.path;

  request(url, function (error, response, html) {
    if (!error && response.statusCode == 200) {
      var $ = cheerio.load(html);

      $(task.file_selector).each(function(i, element){
        //console.log($(element).text());
        result_list.push({
          name: $(element).text(),
          href: task.host + $(element).attr('href')
        });
      });

      async.eachLimit( result_list, 1, function ( file, cb ) {
        if ( !check_file_to_download( task, file.name ) ) return cb();
        console.log("start download", file.name);

        http.get( file.href, function ( res ) {
          var out;
          if ( res.statusCode === 200 ) {
            out = fs.createWriteStream( config.temp_folder + "/" + file.name );
            res.pipe( out );
            out
              .on( "error", cb )
              .on( "finish", function () {
                out.close( function () {
                  preprocessed_file.push( { task: task, file_name: file.name } );
                  cb();
                } );
              } );
          } else {
            console.error( "Not get file: " + file.name );
            cb();
          }
        }).on( 'error', cb );
      }, function ( err ) {
        if ( err ) return console.log( err );
        task_all_downloaded = true;
        console.log('all files were downloaded'); //todo add cb
      });

    } else {
      console.error(error, response.statusCode); //todo add cb
    }
  });
};

var task_processed = function( task, cb ) {
  task_all_downloaded = false;
  task_begin_check( task, function( err ) {
    if ( err ) return cb ( err );

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
        if ( preprocessed_file.idle() && task_all_downloaded ){
          console.log('###all items have been processed', task.name);
          cb();
        }
      };
    })(task, cb);
  });
};

/*
 * MAIN INTERVAL
 */
var parse_cycle = function () {
  var parse_check_interval = config.check_interval;
  if ( parse_cycle_active ) return console.error( "Not finished prev cycle!" );
  console.log('# Start parse cycle', new Date());

  parse_cycle_active = true;
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
      spawn('sh', [ 'db_resetxlog.sh' ]);
      parse_cycle_active = false;
  });
};
parse_cycle();

/*
 tblAvrgSupplDays_30.txt

 # DB structure
 id | code | relevant_code | manufacturer | name | count | price | price_files_id | user_id | fts | delivere
 */