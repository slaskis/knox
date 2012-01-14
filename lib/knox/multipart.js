/*!
 * knox - MultipartUploader
 * Copyright(c) 2010 LearnBoost <dev@learnboost.com>
 * MIT Licensed
 */

var utils = require('./utils')
  , events = require('events')
  , util = require('util')
	, crypto = require('crypto');

/*
 * S3 only allows chunks of 5MB size. The last
 * chunk can be of any size
 */
var MIN_CHUNK_SIZE = (5 * 1024 * 1024);

/**
 * @api private
 */
var MultipartUploader = module.exports = exports = function MultipartUploader(client, filename, headers){
  events.EventEmitter.call(this);

  this.client = client;
  this.filename = filename;
  this.headers = headers;
  this.etags = [];

  this._resetPart();

  var uploader = this;
  var setup = client.request("POST", filename + "?uploads", headers);
  
  setup.on('response', function(res){
    if (res.statusCode !== 200){
      uploader.emit('error', new Error("Preparing MultiPart failed."));
      return;
    }

    utils.readResponseBody(res, function(body){
      var match = /<UploadId>(.*)<\/UploadId>/.exec(body);
      uploader.uploadId = match[1];
      uploader.write = uploader._write;
      
      if(uploader._later){
      	uploader._later();
      	delete uploader._later;
      }

      if(!uploader.writable){
      	uploader._uploadPart();
      }
    });
  });

  setup.end();
};

util.inherits(MultipartUploader, events.EventEmitter);
MultipartUploader.prototype.writable = true;
MultipartUploader.prototype.totalLength = 0;
MultipartUploader.prototype.uploading = 0;
MultipartUploader.prototype.partNumberCounter = 0;

MultipartUploader.prototype._resetPart = function(){
	this.md5 = crypto.createHash("md5");
  this.chunkBuffer = [];
  this.chunkBufferLength = 0;
};

MultipartUploader.prototype._tryComplete = function(buffer, encoding){
	if(this.writable || this.uploading > 0 || this._later){
		return false;
	}

	if(this.totalLength === 0){
		this._completeEmptyFile(this.filename, this.headers);
		return true;
	}

	var uploader = this;
  var url = this.filename + "?uploadId=" + this.uploadId;
	var req = this.client.request("POST", url);

	req.on('response', function(res){
		uploader.emit('response', res);
	});

	req.write("<CompleteMultipartUpload>");

  this.etags.forEach(function(etag, index){
    req.write("<Part>");
    req.write("<PartNumber>" + index + "</PartNumber>");
    req.write("<ETag>" + etag + "</ETag>");
    req.write("</Part>");
  });

  req.write("</CompleteMultipartUpload>");
	req.end();
};

MultipartUploader.prototype._completeEmptyFile = function(){
	var uploader = this;
	var headers = utils.merge({
      Expect: '100-continue'
    , 'Content-Length': '0'
  }, this.headers);

	var req = this.client.put(this.filename, headers);

	req.on('response', function(res){
		uploader.emit('response', res);
	}).end();
};

MultipartUploader.prototype._uploadPart = function(){
  if(this.chunkBufferLength === 0){
  	this._tryComplete();
  	return;
  }

  this.uploading++;

  // lock for closure
  var uploader = this;
  var chunks = this.chunkBuffer;
  var length = this.chunkBufferLength;
  var md5Digest = this.md5.digest("base64")
  var partNumber = (++this.partNumberCounter);
  this._resetPart();

  var url = "" + this.filename
  					   + "?partNumber=" + partNumber
  					   + "&uploadId=" + this.uploadId;
  
  var req = this.client.request("PUT", url, {
  	"Content-Length": length,
  	"MD5-Content": md5Digest
  });

  req.on('response', function(res){
  	if(res.statusCode !== 200){
  		uploader.emit('error', new Error("Failed to upload part: " + partNumber));
  		return;
  	}

  	uploader.uploading--;
  	uploader.etags[partNumber] = res.headers.etag;
  	uploader._tryComplete();
  });

  var chunk;
  while(chunk = chunks.shift()) {
    req.write(chunk);
  }

  req.end();
};

MultipartUploader.prototype._write = function(chunk, encoding){
	if(!Buffer.isBuffer(chunk)){
		chunk = new Buffer(chunk.toString(), encoding)
	}

	this.totalLength += chunk.length;
  this.chunkBufferLength += chunk.length;
  this.chunkBuffer.push(chunk);
  this.md5.update(chunk);

  if (this.chunkBufferLength >= MIN_CHUNK_SIZE) {
  	this._uploadPart();
  	return true; // should we add blocking?
  }

  return true;
};

MultipartUploader.prototype.write = function(){
  var args = arguments;
  var uploader = this;

	this._later = function(){
		uploader.write.apply(uploader, args);
		uploader.emit('drain');
	};

	return false;
};

MultipartUploader.prototype.end = function(){
  if (this.writable) {
    this.writable = false;

    if (!this._later) {
    	this._uploadPart();
  	}

		this.emit('end');	
  }
};