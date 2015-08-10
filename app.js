process.on('uncaughtException', function (err) {
    console.error(err);
    console.error(err.stack);
});
var fs = require("fs");
var iconv = require('iconv-lite');
var parse = require('csv-parse');
var jschardet = require("jschardet");
var http = require('http');
var request = require('request');
var cheerio = require('cheerio');
//var YQL = require("yql");

var res_arr = [];
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

var check_interval_run = false;
//var check_interval = setInterval(function(){
//  if (check_interval_run) return; //todo add write to log
//  check_interval_run = true;
//  console.log("START CHECK INTERVAL |", new Date());
//  read_config();
//  task_processed();
//}, 20 * 1000);

var task_processed = function () {
    var task = config.list.shift();
    console.log("processed task", task.name, "|", new Date());
    switch (task.type) {
        case "http":
            http_processed(task);
            break;
        case "ftp":
            console.log("ftp");
            break;
        default:
            console.error("Unspecified type:", task.type, "| Task:", task.name);
    }
};

var http_processed = function (task) {
    var regexp_file_url, regexp_str;
    var i = 0;
    var result_list = [];
    var url = task.host + task.path;

    if (task.file_extension.length) {
        regexp_str = "(?:[^\\/][\\d\\w\\.]+)+(";
        for (i = 0; i < task.file_extension.length; i++)
            regexp_str += (i === 0 ? task.file_extension[i] : "|" + task.file_extension[i]);
        regexp_str += ")+$";
        regexp_file_url = new RegExp(regexp_str, 'gi');
    }
    request(url, function (error, response, html) {
        if (!error && response.statusCode == 200) {
            var $ = cheerio.load(html);
            $(task.file_selector).each(function(i, element){
                //console.log($(element).text());
                result_list.push($(element).attr('href'));
            });
            console.log("Request result:", result_list.length);
            result_list = result_processed(result_list, regexp_file_url);
            result_list = file_name_processed(result_list, task.host);
            download_file(result_list, function(file_name, is_last){
                console.log('downloaded', file_name, is_last);
            });
        } else {
            console.error(error, response.statusCode);
        }
    });
};

//var http_processed = function(task){
//  var i = 0;
//  var url = task.host + task.path;
//
//  if (task.file_extension.length) {
//    var regexp_str = "(?:[^\/][\d\w\.]+)+(";
//    for (i = 0; i < task.file_extension.length; i++)
//      regexp_str += (i === 0 ? task.file_extension[i] : "|" + task.file_extension[i]);
//    regexp_str += ")+$";
//    var regexp_file_url = new RegExp(regexp_str, 'gi');
//
//    console.log(url, task.file_selector);
//    //var str = 'select * from html where url="' + url + '" and xpath="' + task.file_selector + '"';
//    var xpath = '(//A|//a)[starts-with(@href, "/files/")]';
//    var str = "select * from html where url='http://91.197.10.216/files/' and xpath='"+xpath+"'";
//    console.log(str);
//    //new YQL.exec('select * from html where url="' + url + '" and xpath="' + task.file_selector + '"', function(response) {
//    new YQL.exec(str, function(response) {
//      var list;
//      if (response.error || !response.query.results)
//        return console.error("YQL ERROR", response);
//      list = response.query.results.a;
//      console.log("YQL result:", list.length);
//      return;
//      list = result_processed(list, regexp_file_url);
//      list = file_name_processed(list);
//      download_file(list, function(file_name, is_last){
//        console.log(downloaded, file_name, is_last);
//      });
//    });
//  }
//};

//var url = "http://91.197.10.216/files/";
//var host = "http://91.197.10.216";
//var selector = 'a[href=\'/\']';
//var xpath = "(//A|//a)[starts-with(@href, '/files/')]";
//var regexp_file_url = /(?:[^\/][\d\w\.]+)+(csv|zip|xls)+$/g;


var result_processed = function (array, regexp_file_url) {
    var f, j, len, ret, v;
    ret = [];
    for (j = 0, len = array.length; j < len; j++) {
        v = array[j];
        f = regexp_file_url.exec(v);
        if (f !== null ? f.length : void 0) {
            ret.push({
                href: v,
                file_name: f[0]
            });
        }
    }
    //todo add file_list check
    return ret;
};

var file_name_processed = function (array, host) {
    var j, len, ret, v;
    ret = [];
    for (j = 0, len = array.length; j < len; j++) { //todo add file_mask check
        v = array[j];
        if (v.href[0] === "/") {
            v.href = host + v.href;
        }
        ret.push(v);
    }
    return ret;
};

var download_file = function (file_url_list, cb) {
    var v;
    var len = file_url_list.length;
    var count = 0;
    var transmit_count = 0;
    var out_file_list = [];
    console.log("Start interval", file_url_list.length);
    var inv = setInterval(function(){
        if (!file_url_list.length)
           return clearInterval(inv);
        if (transmit_count >= config.max_file_transmission)
            return;
        v = file_url_list.shift();
        (function (v) {
            var i = transmit_count++;
            out_file_list[i] = fs.createWriteStream("./temp/" + v.file_name);
            http.get(v.href, function (res) {
                count++;
                transmit_count--;
                res.pipe(out_file_list[i]);
                if (count !== len)
                    cb(v.file_name, false);
                else
                    cb(v.file_name, true);
            }).on('error', function (e) {
                count++;
                transmit_count--;
                console.error("Get error:", count, v.file_name, e.message);
                if (count === len)
                    cb(null, true);
            });
        })(v);
    },500);
};

task_processed();
//fs.readFile('EURO_авиа_BMW_GR[3143]_Final.csv', function (err, data) {
//  if (err) throw err;
//  var charsets = jschardet.detect(data);
//  console.log(charsets);
//  var data_decoded = iconv.decode(data, charsets.encoding);
//  var array = CSVtoArray(data_decoded);
//  parse(data_decoded, {
//  	delimiter: ";"
//  }, function(err, output){
//  	if (err) throw err;
//  	var len = output.length;
//  	var temp = []
//  	for (var i = 0; i < len; i++) {
//  	  temp = output[i];
//  	  res_arr.push([temp[0], temp[1], temp[3], temp[4]]);
//  	};
//  	console.log(output.length, output[0], output[output.length-1]);
//  	write_result();
//  });
//});

var write_result = function () {
    var data = "";
    var len = res_arr.length;
    var temp_len = res_arr[0].length;
    var i = 0, j = 0;
    for (i = 0; i < len; i++) {
        for (j = 0; j < temp_len; j++) {
            data += res_arr[i][j] + ";";
        }
        data += "\n";
    }
    fs.writeFile('message.txt', data, function (err) {
        if (err) throw err;
        console.log('It\'s saved!');
    });
};

var CSVtoArray = function (text) {
    var re_valid = /^\s*(?:'[^'\\]*(?:\\[\S\s][^'\\]*)*'|"[^"\\]*(?:\\[\S\s][^"\\]*)*"|[^,'"\s\\]*(?:\s+[^,'"\s\\]+)*)\s*(?:,\s*(?:'[^'\\]*(?:\\[\S\s][^'\\]*)*'|"[^"\\]*(?:\\[\S\s][^"\\]*)*"|[^,'"\s\\]*(?:\s+[^,'"\s\\]+)*)\s*)*$/;
    var re_value = /(?!\s*$)\s*(?:'([^'\\]*(?:\\[\S\s][^'\\]*)*)'|"([^"\\]*(?:\\[\S\s][^"\\]*)*)"|([^,'"\s\\]*(?:\s+[^,'"\s\\]+)*))\s*(?:,|$)/g;
    // Return NULL if input string is not well formed CSV string.
    if (!re_valid.test(text)) return null;
    var a = [];           // Initialize array to receive values.
    text.replace(re_value, // "Walk" the string using replace with callback.
        function (m0, m1, m2, m3) {
            // Remove backslash from \' in single quoted values.
            if (m1 !== undefined) a.push(m1.replace(/\\'/g, "'"));
            // Remove backslash from \" in double quoted values.
            else if (m2 !== undefined) a.push(m2.replace(/\\"/g, '"'));
            else if (m3 !== undefined) a.push(m3);
            return ''; // Return empty string.
        });
    // Handle special case of empty last value.
    if (/,\s*$/.test(text)) a.push('');
    return a;
};