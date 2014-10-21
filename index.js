var IronMQ        = require("iron_mq");
var Stream        = require("stream");
var _             = require("lodash");
var util          = require("util");
var EventEmitter  = require("events").EventEmitter;
var JsonParser    = require("./jsonparser");
var debug         = require("debug")("ironmq-queue");
var fetcherDebug  = require("debug")("fetcher");
var systemDebug   = require("debug")("system");
var inspect       = require("util").inspect;
var EventEmitter  = require("events").EventEmitter;
var inherits      = require("util").inherits;
var async         = require("async");

util.inherits(Queue, Stream.Readable);
util.inherits(Sink, Stream.Writable);

function IronStream(config) {
  if(!config.projectId || !config.projectToken) {
    throw new Error("Must include both `projectId` and `projectToken`");
  }
  if(!(this instanceof IronStream)) {
    return new IronStream(config);
  }
  this.MQ = new IronMQ.Client(
                _.merge({project_id: config.projectId, token: config.projectToken},
                _.omit(config, "projectId", "projectToken")));
  this.queues = {};
}

/*
 * @param name {String}: Name of the queue to connect to
 * @param options {Object}:

        options.ironmq.concurrentRequests {Num} - Number of fetch requests to issue in parallel. Once a fetch
                                           returns results all outstanding requests are completed and
                                           no fetches are issued until the internal fetch queue is
                                           pushed downstream.
        options.ironmq.* {Num} - Any set of options accepted by the ironmq npm client. https://www.npmjs.org/package/iron_mq

        options.stream.*  - Any set of options accepted by Stream.Readable. Will override defaults.

*/


IronStream.prototype.queue = function(name, options) {
  this.queues[name] = this.queues[name]
    || new Queue(this, name, options);
  return this.queues[name];
};


function Queue(ironStream, name, options) {
  var me = this;
  var defaultOptions = {
    ironmq: {
      concurrentRequests: 5,
      n: 10
    },
    stream: {
      objectMode: true
    }
  };
  var options = _.merge(defaultOptions, (options || {}));
  Stream.Readable.call(this, _.extend(options.stream));
  this.name = name;
  this.options = options;
  this.q = ironStream.MQ.queue(name);
  this.running = true;
  this.messages = [];
  this.fetcher = new Fetcher(me.q.get.bind(me.q, options.ironmq), removeAndReturn(options.ironmq, "concurrentRequests"));

  /* add default fetcher handlers */
  this.fetcher.on("results", function(results) {
    if(!_.isEmpty(results)) {
      me._addMessagesToQueue(results);
    }
  });
  this.fetcher.on("error", function(err) {
    me.emit("queueError", err);
  });
  this.firstMessageListener = function(results) {
    if(!_.isEmpty(results)) {
      me._pushOneMessage();
      if(!_.isEmpty(me.messages)) me.fetcher.stop();
    }
  };
}


Queue.prototype._pushOneMessage = function() {
  debug("Pushing one message downstream");
  if(!this.push(this.messages.shift())) {
    debug("Downstream backpressure detected");
  }
  debug("Internal messages length: " + this.messages.length);
}

Queue.prototype._read = function(s) {
  systemDebug("System called _read");
  var me = this;
  if(_.isEmpty(this.messages)) {
    if(!this.fetcher.running) {
      this.fetcher.start();
    }
    this.fetcher.once("results", this.firstMessageListener);
  } else {
    if(this.fetcher.running) {
      this.fetcher.stop();
    }
    me._pushOneMessage();
  }
};

Queue.prototype._isFetching = function() {
  return !!this.__i;
}

Queue.prototype._addMessagesToQueue = function(messages) {
  messages = _.isArray(messages) ? messages : [messages];
  debug("Adding " + messages.length + " messages to queue");
  this.messages = this.messages.concat(messages);
};

/*
 * Function: resume
 *
 * Used to resume a queue after calling stop.
*/
Queue.prototype.resume = function() {
  debug("Resuming...");
  this.running = true;
};

Queue.prototype.onFetchError = function(f) {
  this.on("queueError", f);
};

/*
 * Function: stopFetching
 *
 * Client-safe way to stop the underlying polling of IronMQ.
 *
*/
Queue.prototype.stopFetching = function() {
  this.fetcher.shutdown();
};


Queue.prototype.resetMessages = function() {
  this.messages = [];
};

/*
 * Provides a writable stream for ironmq manipulation. Messages written to the
 * stream will be deleted from the remote queue.

 Sink should be used downstream of an IronmqStream instance.

 @param instance of IronStream.Queue.  This is returned when .queue()
        is invoked on an instantiated IronStream object.

  @param options {Object} Any options accepted by node Stream.Writable


    options.deleteInBatchesOf:  {Num} Messages will be deleted in batches of this much. DEFAULT is 1
    options.stream.*            Any options accepted by node Stream.Writable



*/
function Sink(ironmqQueue, options) {
  var defaultOptions = {
    objectMode: true,
    decodeStrings: false
  };
  Stream.Writable.call(this, _.merge(defaultOptions, options.stream));
  this.q = ironmqQueue.q;
  this._deleteInBatchesOf = options.deleteInBatchesOf || 1;
  this._toDelete = [];
}


Sink.prototype._write = function(message, enc, next) {
  var deletingMessages;
  var me = this;
  if(!message.id) {
    return this.emit("deleteError", new Error("Message does not have an `id` property"), message);
  }
  this._toDelete.push(message);
  if(this._toDelete.length < this._deleteInBatchesOf) {
    this._toDelete.push(message);
    me.emit("deletePending", message.id);
    next();
  } else {
    //slice up to batch number off
    deletingMessages = _.first(this._toDelete, this._deleteInBatchesOf).map(function(message) {
      return message.id;
    });
    this._toDelete = _.rest(this._toDelete, this._deleteInBatchesOf);
    this.q.del_multiple(deletingMessages, function(err) {
      if(err) {
        //readd back to _toDelete queue?
        me.emit("deleteError", err, deletingMessages);
      }
      debug("Deleted " + deletingMessages.length + " messages");
      me.emit("deleted", deletingMessages);
      next();
    });
  }
};

Sink.prototype.onDeleteError = function(f) {
  this.on("deleteError", f);
};



/* Fetcher */

inherits(Fetcher, EventEmitter);


/* fetch should by asynchronous. */
function Fetcher (fetch, concurrentRequestLimit) {
  this.fetch = fetch;
  this.running = false;
  this.concurrentRequestLimit = concurrentRequestLimit;
  this._outstandingRequests = 0;
}

Fetcher.prototype.start = function() {
  var me = this;
  this.running = true;
  debug("Starting fetcher");
  if(!this.__i && !this.shuttingDown) {
    this.__i = setInterval(function() {
      me._fetch();
    }, 5);
  }
}

Fetcher.prototype._fetch = function() {
  var me = this;
  while(this._outstandingRequests < this.concurrentRequestLimit) {
    fetcherDebug("Fetching. Outstanding requests: " + this._outstandingRequests);
    this._outstandingRequests++;
    me.fetch(function(err, results) {
      me._outstandingRequests--;
      if(err) {
        fetcherDebug("Error in fetch: " + err.message);
        return me.emit("error", err);
      }
      fetcherDebug("Successful fetch");
      me.emit("results", results);
    });
  }
};

Fetcher.prototype.stop = function() {
  fetcherDebug("Stopping fetcher")
  this.running = false;
  if(this.__i) {
    clearInterval(this.__i);
    this.__i = null;
  }
};

Fetcher.prototype.shutdown = function() {
  fetcherDebug("Shutting down");
  this.shuttingDown = true;
  this.stop();
}



exports.IronStream = IronStream;
exports.Queue = Queue;
exports.Sink = Sink;
exports.Fetcher = Fetcher;

/*
  @param ironmqStream {Stream} A configured ironmq stream.
  @param onParseError {Function} Called when there's a parsing error.
*/

exports.parseJson = function(ironmqStream, options) {
  options = options || {};
  var parsedStream = new JsonParser(_.merge({parseField: "body", enrichWith: ["id"]}, options));
  parsedStream.on("parseError", options.onParseError || function() {});
  return ironmqStream
            .pipe(parsedStream);
};

exports.useStub = function(stub) {
  IronMQ = stub;
};

var removeAndReturn = function(obj, prop) {
  var p = obj[prop];
  delete obj[prop];
  return p;
}; 
