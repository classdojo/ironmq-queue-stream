var IronMQ        = require("iron_mq");
var Stream        = require("stream");
var _             = require("lodash");
var util          = require("util");
var EventEmitter  = require("events").EventEmitter;
var JsonParser    = require("./jsonparser");
var debug         = require("debug")("ironmq-queue");
var inspect       = require("util").inspect;
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
          options.checkEvery {Num} - Interval with which to check ironMQ in ms.
          options.maxMessagesPerEvent {Num} - The maximum number of messages to return in any given push.
*/


IronStream.prototype.queue = function(name, options) {
  var options = _.merge({
      checkEvery: 1000,
      maxMessagesPerEvent: 10
    }, (options || {}));
  this.queues[name] = this.queues[name]
    || new Queue(this, name, options);
  return this.queues[name];
};


function Queue(ironStream, name, options) {
  Stream.Readable.call(this, {objectMode: true});
  this.name = name;
  this.options = options;
  this.q = ironStream.MQ.queue(name);
  this.running = true;
  this.messages = [];
  this.__flush = false;
}

Queue.prototype._read = function(s) {
  this.__flush = true;
  this._startFetching();
};

/*
 * Method: _startFetching
 *
 * Starts polling of IronMQ for messages.
*/
Queue.prototype._startFetching = function() {
  var options;
  var me = this;
  if(!this.__i && this.running) {
    options = {n: this.options.maxMessagesPerEvent};
    this.__i = setInterval(function() {
      me.q.get(options, function(error, messages) {
        if(error) {
          debug("Error fetching messages:", error);
          return me.emit("queueError", error);
        }
        debug("Fetched messages:", messages);
        if(_.isEmpty(messages)) return;
        me._addMessagesToQueue(messages);
        if(me.__flush) {
          me._startMessageConsumer();
        }
      });
    }, this.options.checkEvery);
  }
};

/*
 *  Method: _startMessageConsumer
 *
 *  Handles retry logic in the event of this.push failing.
*/ 
Queue.prototype._startMessageConsumer = function() {
  var message;
  var me = this;
  /* 
   *  Try pushing as many messages downstream as possible.
  */
  if(!this.__consumerInterval) {
    debug("Starting internal queue consumer");
    /* 
     * Let's allow node to service downstream I/O instead
     * of blocking in a while(true) loop
    */
    this.__consumerInterval =  setInterval(function() {
      if(!me._pushOneMessageDownstream()) {
        debug("Stopping internal queue consumer");
        me.__flush = false;
        clearInterval(me.__consumerInterval);
        me.__consumerInterval = null;
      };
    }, 0);
  }
};

Queue.prototype._addMessagesToQueue = function(messages) {
  messages = _.isArray(messages) ? messages : [messages];
  debug("Adding " + messages.length + " messages to queue");
  this.messages = this.messages.concat(messages);
};

/* 
 * Synchronous.
 *
 * Attempts to push one message downstream.
 * Returns truthy if successful, false otherwise.
*/
Queue.prototype._pushOneMessageDownstream = function() {
  debug("Pushing one message downstream");
  return !_.isEmpty(this.messages) && this.push(this.messages.shift());
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
 * Stops the underlying polling of IronMQ.
*/
Queue.prototype.stopFetching = function() {
  debug("Stop fetching");
  if(this.__i) {
    clearInterval(this.__i);
    this.__i = null;
    this.running = false;
  }
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
*/
function Sink(ironmqQueue) {
  Stream.Writable.call(this, {objectMode: true, decodeStrings: false});
  this.q = ironmqQueue.q;
}


Sink.prototype._write = function(message, enc, next) {
  if(!message.id) {
    return this.emit("deleteError", new Error("Message does not have an `id` property"), message);
  }
  this.q.del(message.id, function(err) {
    if(err) {
      this.emit("deleteError", error);
    }
    debug("Deleted message:", message);
    next();
  });
};

Sink.prototype.onDeleteError = function(f) {
  this.on("deleteError", f);
};

exports.IronStream = IronStream;
exports.Queue = Queue;
exports.Sink = Sink;

/*
  @param ironmqStream {Stream} A configured ironmq stream.
  @param onParseError {Function} Called when there's a parsing error.
*/

exports.parseJson = function(ironmqStream, onParseError) {
  parsedStream = new JsonParser({parseField: "body", enrichWith: ["id"]});
  parsedStream.on("parseError", onParseError || function() {});
  return ironmqStream
            .pipe(parsedStream);
};


exports.useStub = function(stub) {
  IronMQ = stub;
};
