var IronMQ  = require("iron_mq");
var Stream = require("stream");
var _       = require("lodash");
var util = require("util");
var EventEmitter = require("events").EventEmitter;
var JsonParser = require("./jsonparser");

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
  Stream.Readable.call(this, {objectMode: true, decodeStrings: false});
  this.name = name;
  this.options = options;
  this.q = ironStream.MQ.queue(name);
  this.running = true;
  this.messages = [];
}

Queue.prototype._read = function() {
  var me = this;
  var options = {n: this.options.maxMessagesPerEvent};
  if(!this.__i && this.running) {
    this.__i = setInterval(function() {
      me.q.get(options, function(error, messages) {
        if(error) return me.emit("queueError", error);
        if(!messages) return;
        messages = _.isArray(messages) ? messages : [messages]; 
        me.messages.concat(messages);
      });
    }, this.options.checkEvery);
  }
  if(this.messages.length) {
    if(!this.push(this.messages)) {
      this.pause();
      /* 
        Slight hack since this was a system defined pause. System will resume next
        time _read is called.
      */
      this.resume();
    }; //handle backpressure
    this.resetMessages();
  }
};


/*
 * Function: resume
 *
 * Used to resume a queue after calling stop.
*/
Queue.prototype.resume = function() {
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
