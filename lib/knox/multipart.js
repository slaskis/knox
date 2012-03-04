/*!
 * knox - Multipart
 * Copyright(c) 2012 LearnBoost <dev@learnboost.com>
 * MIT Licensed
 */

var utils = require('./utils')
  , EventEmitter = require('events').EventEmitter
  , util = require('util')
  , crypto = require('crypto');


module.exports = exports = Multipart;

/*
 * S3 only allows chunks of 5MB size. The last
 * chunk can be of any size
 */
var MIN_CHUNK_SIZE = (5 * 1024 * 1024);

function Multipart(client, filename, headers){
  this.client = client;
  this.filename = filename;
  this.headers = headers;
  this.etags = [];
  this.paused = true;
  this.writable = true;
  this.buffer = [];
  this.parts = 0;
  this.length = 0;
  this.pending = 0;

  this.reset();

  var self = this
    , setup = client.request('POST',filename+'?uploads', headers);

  setup.on('response',function(res){
    if( res.statusCode !== 200 ){
      return self.emit('error', new Error("Preparing MultiPart failed with "+res.statusCode));
    }

    buffer(res, function(body){
      var match = /<UploadId>(.*)<\/UploadId>/.exec(body);
      self.uploadId = match[1];
      self.paused = false;

      // write any buffered chunks
      var chunk;
      while(chunk = self.buffer.shift())
        self.write(chunk);

      if( self.chunkBufferLength )
        self.uploadPart();
    })
  }).end()
}

Multipart.prototype = {
  __proto__: EventEmitter.prototype,

  reset: function(){
    this.md5 = crypto.createHash('md5');
    this.chunkBuffer = [];
    this.chunkBufferLength = 0;
  },

  write: function(chunk,enc){
    if( !Buffer.isBuffer(chunk) )
      chunk = new Buffer(chunk.toString(),encoding);

    // buffer chunks until initial response from s3
    if( this.paused ){
      this.buffer.push(chunk);
      return false;
    }

    // not paused anymore, buffer into s3 chunks
    this.length += chunk.length;
    this.chunkBufferLength += chunk.length;
    this.chunkBuffer.push(chunk);
    this.md5.update(chunk);

    // it's s3 size, upload it!
    if( this.chunkBufferLength >= MIN_CHUNK_SIZE )
      return this.uploadPart();

    return true;
  },

  end: function(chunk,enc){
    if( chunk )
      this.write(chunk,enc);

    if( this.writable ){
      this.writable = false;
      
      if( this.chunkBufferLength )
        this.uploadPart()

      this.emit('end')
    }
  },

  uploadPart: function(){
    // nothing to write
    if( this.chunkBufferLength === 0 )
      return this.tryComplete();

    // keep track of the uploads
    this.pending++;

    // store references before reset
    var buf = this.chunkBuffer
      , len = this.chunkBufferLength
      , md5 = this.md5.digest('base64')
      , pnr = ++this.parts;

    // start the next s3 chunk
    this.reset();

    var url = this.filename 
              + '?partNumber=' + pnr
              + '&uploadId=' + this.uploadId;

    var req = this.client.request('PUT', url, {
      'Content-Length': len,
      'MD5-Content': md5
    })

    var self = this;
    req.on('response',function(res){
      if( res.statusCode !== 200 ){
        return self.emit('error', new Error('Failed to upload part '+pnr+', got status '+res.statusCode))
      }

      self.pending--;
      self.etags[pnr] = res.headers.etag;
      self.tryComplete();
    })

    var chunk;
    while(chunk = buf.shift())
      req.write(chunk);

    req.end();
  },

  tryComplete: function(){
    if( this.paused || this.pending )
      return false;

    if( this.length == 0 )
      return this.completeEmptyFile();

    return this.complete();
  },

  complete: function(){
    var self = this
      , url = this.filename + '?uploadId=' + this.uploadId
      , req = this.client.request('POST',url);

    req.on('response', function(res){
      self.emit('response', res);
    })

    req.write("<CompleteMultipartUpload>");
    this.etags.forEach(function(etag, index){
      req.write("<Part>");
      req.write("<PartNumber>" + index + "</PartNumber>");
      req.write("<ETag>" + etag + "</ETag>");
      req.write("</Part>");
    });
    req.write("</CompleteMultipartUpload>");
    req.end();
    return true;
  },

  completeEmptyFile: function(){
    var self = this
      , headers = utils.merge({
          'Expect': '100-continue',
          'Content-Length': 0
        }, this.headers)
      , req = this.client.put(this.filename, headers);

    req.on('response',function(res){
      self.emit('response',res);
    })
    req.end();
    return true;
  }

}


function buffer(res,fn){
  var buf = '';
  res.setEncoding('utf8');
  res.on('data',function(data){ buf += data });
  res.on('end', function(){ fn(buf) })
}