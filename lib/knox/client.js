
/*!
 * knox - Client
 * Copyright(c) 2010 LearnBoost <dev@learnboost.com>
 * MIT Licensed
 */

/**
 * Module dependencies.
 */

var utils = require('./utils')
  , auth = require('./auth')
  , http = require('http')
  , url = require('url')
  , join = require('path').join
  , mime = require('./mime')
  , fs = require('fs')
  , crypto = require('crypto')
  , xml2json = require('xml2json');

/**
 * Initialize a `Client` with the given `options`.
 *
 * Required:
 *
 *  - `key`     amazon api key
 *  - `secret`  amazon secret
 *  - `bucket`  bucket name string, ex: "learnboost"
 *
 * @param {Object} options
 * @api public
 */

var Client = module.exports = exports = function Client(options) {
  this.endpoint = 's3.amazonaws.com';
  this.port = 80;
  if (!options.key) throw new Error('aws "key" required');
  if (!options.secret) throw new Error('aws "secret" required');
  if (!options.bucket) throw new Error('aws "bucket" required');
  utils.merge(this, options);
};

/**
 * Request with `filename` the given `method`, and optional `headers`.
 *
 * @param {String} method
 * @param {String} filename
 * @param {Object} headers
 * @return {ClientRequest}
 * @api private
 */

Client.prototype.request = function(method, filename, headers){
  var options = { host: this.endpoint, port: this.port }
    , date = new Date
    , headers = headers || {};

  // Default headers
  utils.merge(headers, {
      Date: date.toUTCString()
    , Host: this.endpoint
  });

  // Authorization header
  headers.Authorization = auth.authorization({
      key: this.key
    , secret: this.secret
    , verb: method
    , date: date
    , resource: auth.canonicalizeResource(join('/', this.bucket, filename))
    , contentType: headers['Content-Type']
    , md5: headers['Content-MD5'] || ''
    , amazonHeaders: auth.canonicalizeHeaders(headers)
  });

  // Issue request
  options.method = method;
  options.path = join('/', this.bucket, filename);
  options.headers = headers;
  var req = http.request(options);
  req.url = this.url(filename);

  return req;
};

/**
 * PUT data to `filename` with optional `headers`.
 *
 * Example:
 *
 *     // Fetch the size
 *     fs.stat('Readme.md', function(err, stat){
 *      // Create our request
 *      var req = client.put('/test/Readme.md', {
 *          'Content-Length': stat.size
 *        , 'Content-Type': 'text/plain'
 *      });
 *      fs.readFile('Readme.md', function(err, buf){
 *        // Output response
 *        req.on('response', function(res){
 *          console.log(res.statusCode);
 *          console.log(res.headers);
 *          res.on('data', function(chunk){
 *            console.log(chunk.toString());
 *          });
 *        });
 *        // Send the request with the file's Buffer obj
 *        req.end(buf);
 *      });
 *     });
 *
 * @param {String} filename
 * @param {Object} headers
 * @return {ClientRequest}
 * @api public
 */

Client.prototype.put = function(filename, headers){
  headers = utils.merge({
      Expect: '100-continue'
    , 'x-amz-acl': 'public-read'
  }, headers || {});
  return this.request('PUT', filename, headers);
};


/**
 * Initiates multipart upload.
 * @param {String} filename The name of the object on Amazon S3 service.
 * @param {Object} headers Additional headers used in HTTP request to S3 service.
 * @param {Function} fn A callback that accepts two parameters: err that holds exception info, and ir - multipart upload initiation result.
 * @api public
 */
Client.prototype.beginUpload = function(filename, headers, fn){
  if(typeof headers == 'function') { fn = headers; headers = {}; }

  filename += '?uploads';
  var req = this.request('POST', filename, headers);

  req.on('response', function(response){
    if(response.statusCode !== 200) return fn(new Error('Knox.beginUpload: ' + response.statusCode));

    var data = '';
    response.on('data', function(chunk) {
      data += chunk;
    });
    response.on('end', function() {
      var json = xml2json.toJson(data);
      fn(null, json);
    });
  });
  req.end();
};

/**
 * Lists the parts that have been uploaded for a specific multipart upload. 
 * @param {String} filename Name of the remote object.
 * @param parameters May be {String} or {Object}. Specifies Upload ID returned by begin_upload function; or set
 * 					additional query parameters.
 * @param {Object} headers Additional request headers.
 * @param {Function} fn Callback function that accepts two parameters: err - information about error in request/response; linfo - upload status.
 * @api public
 */
Client.prototype.getParts = function(filename, parameters, headers, fn){
  if(typeof headers == 'function') {
    fn = headers;
    headers = {};
  }

  filename += '?uploadId=' + parameters['uploadId'];

  if(parameters['max-parts']) {
    filename += '&max-parts=' + parameters['max-parts'];
  }
  if(parameters['part-number-marker']) {
    filename += '&part-number-marker=' + parameters['part-number-marker'];
  }

  var req = this.request('GET', filename, headers);
	req.on('response', function(response){
		if(response.statusCode != 200) return fn(new Error('Knox.getParts: ' + response.statusCode));
    var data = '';
    response.on('data', function(chunk) {
      data += chunk;
    })
    response.on('end', function() {
      var json = xml2json.toJson(data);
      fn (null, json);
    });
	});
	req.end();
};


/**
 * Aborts a multipart upload.
 * @param {String} filename Name of the remote object.
 * @param {String} uploadId Multipart upload identifier.
 * @param {Object} headers Additional request headers.
 * @param {Function} fn Callback function that accepts true, if upload is aborted successfully; otherwise, false.
 * @api public
 */
Client.prototype.abortUpload = function(filename, uploadId, headers, fn){
	if(typeof headers == 'function') {
    fn = headers;
    headers = {}; 
  }
	filename += '&uploadId=' + uploadId;
	var req = this.request('DELETE', filename, headers);
	req.on('response', function(response){
    if (response.statusCode === 200 || response.statusCode === 204) return fn(null);
    fn(new Error('Knox.abortUpload: ' + response.statusCode));
  });
	req.end();
};

/**
 * Completes a multipart upload by assembling previously uploaded parts. 
 * @param {String} filename Name of the remote object.
 * @param {String} uploadId Multipart upload identifier.
 * @param {Array} parts An array of parts to complete. Each element in the array has the following structure:
 * 					partNumber - an integer that identifier number of the file part, etag - entity tag that identifies the object's data.
 * @param {Object} headers Additional request headers.
 * @param {Function} fn Callback function that accepts two parameters: err - exception information, rinfo - response data.
 * @api public
 */
Client.prototype.completeUpload = function(filename, uploadId, parts, headers, fn){
  if(typeof headers == 'function') {
    fn = headers; headers = {};
  }

  filename += '?uploadId=' + uploadId;

  var doc = '<CompleteMultipartUpload>'
  for(var i in parts) {
    doc += '<Part>';
    doc += '<PartNumber>' + parts[i].partNumber + '</PartNumber>';
    doc += '<ETag>"' + parts[i].etag + '"</ETag>';
    doc += '</Part>\n\r';
  }

  doc += '</CompleteMultipartUpload>';

  doc = new Buffer(doc, 'utf8');
  headers['Content-Length'] = doc.length;
  headers['Content-Type'] = 'text/xml';
  var req = this.request('POST', filename, headers);

  req.on('response', function(response){
    if(response.statusCode === 200) return fn(null, data);
    fn(new Error('Knox.completeUpload: ' + response.statusCode));
  });
  req.end(doc);
};


/**
 * Uploads part of the file to the Amazon S3 service.
 * @param {String} filename Name of the remote object.
 * @param {Integer} partNumber The number of the part to upload.
 * @param {String} uploadId Upload identifier.
 * @param {Object} buf A block of the data to send.
 * @param {Function} A callback that receives two parameters: err - exception information, pinfo - information about uploaded part.
 * @api public
 */
Client.prototype.putPart = function(filename, buffer, partNumber, uploadId, fn) {
  filename += '?partNumber=' + partNumber;
  filename += '&uploadId=' + uploadId;

  var req = this.request('PUT', filename, {'Content-Length': buffer.length,
    'Content-MD5': crypto.createHash('md5').update(buffer).digest('base64'),
    'Expect': '100-continue'});

  req.on('response', function(response) {
    if(response.statusCode == 200) { 
      fn(null, {'etag': JSON.parse(response.headers['etag']), 'partNumber': partNumber});
    } else {
      fn(new Error('Knox.putPart: ' + response.statusCode));
    }
  });
  req.end(buf);
};

/**
 * PUT the file at `src` to `filename`, with callback `fn`
 * receiving a possible exception, and the response object.
 *
 * NOTE: this method reads the _entire_ file into memory using
 * fs.readFile(), and is not recommended or large files.
 *
 * Example:
 *
 *    client
 *     .putFile('package.json', '/test/package.json', function(err, res){
 *       if (err) throw err;
 *       console.log(res.statusCode);
 *       console.log(res.headers);
 *     });
 *
 * @param {String} src
 * @param {String} filename
 * @param {Object|Function} headers
 * @param {Function} fn
 * @api public
 */

Client.prototype.putFile = function(src, filename, headers, fn) {
  var self = this;
  if ('function' == typeof headers) {
    fn = headers;
    headers = {};
  };
  fs.readFile(src, function(err, buf){
    if (err) return fn(err);
    headers = utils.merge({
        'Content-Length': buf.length
      , 'Content-Type': mime.lookup(src)
      , 'Content-MD5': crypto.createHash('md5').update(buf).digest('base64')
    }, headers);
    self.put(filename, headers).on('response', function(res){
      fn(null, res);
    }).end(buf);
  });
};

/**
 * PUT the given `stream` as `filename` with optional `headers`.
 *
 * @param {Stream} stream
 * @param {String} filename
 * @param {Object|Function} headers
 * @param {Function} fn
 * @api public
 */

Client.prototype.putStream = function(stream, filename, headers, fn){
  var self = this;
  if ('function' == typeof headers) {
    fn = headers;
    headers = {};
  };
  fs.stat(stream.path, function(err, stat){
    if (err) return fn(err);
    // TODO: sys.pump() wtf?
    var req = self.put(filename, utils.merge({
        'Content-Length': stat.size
      , 'Content-Type': mime.lookup(stream.path)
    }, headers));
    req.on('response', function(res){
      fn(null, res);
    });
    stream
      .on('error', function(err){fn(err); })
      .on('data', function(chunk){ req.write(chunk); })
      .on('end', function(){ req.end(); });
  });
};

/**
 * GET `filename` with optional `headers`.
 *
 * @param {String} filename
 * @param {Object} headers
 * @return {ClientRequest}
 * @api public
 */

Client.prototype.get = function(filename, headers){
  return this.request('GET', filename, headers);
};

/**
 * GET `filename` with optional `headers` and callback `fn`
 * with a possible exception and the response.
 *
 * @param {String} filename
 * @param {Object|Function} headers
 * @param {Function} fn
 * @api public
 */

Client.prototype.getFile = function(filename, headers, fn){
  if ('function' == typeof headers) {
    fn = headers;
    headers = {};
  }
  return this.get(filename, headers).on('response', function(res){
    fn(null, res);
  }).end();
};

/**
 * Issue a HEAD request on `filename` with optional `headers.
 *
 * @param {String} filename
 * @param {Object} headers
 * @return {ClientRequest}
 * @api public
 */

Client.prototype.head = function(filename, headers){
  return this.request('HEAD', filename, headers);
};

/**
 * Issue a HEAD request on `filename` with optional `headers`
 * and callback `fn` with a possible exception and the response.
 *
 * @param {String} filename
 * @param {Object|Function} headers
 * @param {Function} fn
 * @api public
 */

Client.prototype.headFile = function(filename, headers, fn){
  if ('function' == typeof headers) {
    fn = headers;
    headers = {};
  }
  return this.head(filename, headers).on('response', function(res){
    fn(null, res);
  }).end();
};

/**
 * Obtains information about uploaded object.
 * @param {String} filename
 * @param {Object|Function} headers
 * @param {Function} fn
 */
Client.prototype.fileInfo = function(filename, headers, fn){
	this.headFile(filename, headers, function(res){
		if(res.statusCode == 200) fn({'etag': JSON.parse(res.headers['etag']), 'size': JSON.parse(res.headers['content-length']), 'modified': res.headers['last-modified']});
		else fn(null);
	});
};

/**
 * DELETE `filename` with optional `headers.
 *
 * @param {String} filename
 * @param {Object} headers
 * @return {ClientRequest}
 * @api public
 */

Client.prototype.del = function(filename, headers){
  return this.request('DELETE', filename, headers);
};

/**
 * DELETE `filename` with optional `headers`
 * and callback `fn` with a possible exception and the response.
 *
 * @param {String} filename
 * @param {Object|Function} headers
 * @param {Function} fn
 * @api public
 */

Client.prototype.deleteFile = function(filename, headers, fn){
  if ('function' == typeof headers) {
    fn = headers;
    headers = {};
  }
  return this.del(filename, headers).on('response', function(res){
    fn(null, res);
  }).end();
};

/**
 * Return a url to the given `filename`.
 *
 * @param {String} filename
 * @return {String}
 * @api public
 */

Client.prototype.url =
Client.prototype.http = function(filename){
  return 'http://' + join(this.endpoint, this.bucket, filename);
};

/**
 * Return an HTTPS url to the given `filename`.
 *
 * @param {String} filename
 * @return {String}
 * @api public
 */

Client.prototype.https = function(filename){
  return 'https://' + join(this.endpoint, filename);
};

/**
 * Return an S3 presigned url to the given `filename`.
 *
 * @param {String} filename
 * @param {Date} expiration
 * @return {String}
 * @api public
 */

Client.prototype.signedUrl = function(filename, expiration){
  var epoch = Math.floor(expiration.getTime()/1000);
  var signature = auth.signQuery({
    secret: this.secret,
    date: epoch,
    resource: '/' + this.bucket + url.parse(filename).pathname
  });

  return this.url(filename) +
    '?Expires=' + epoch +
    '&AWSAccessKeyId=' + this.key +
    '&Signature=' + encodeURIComponent(signature);
};

/**
 * Shortcut for `new Client()`.
 *
 * @param {Object} options
 * @see Client()
 * @api public
 */

exports.createClient = function(options){
  return new Client(options);
};
