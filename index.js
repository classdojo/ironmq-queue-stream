var IronMQ  = require("iron_mq");
var through = require("through");
var _       = require("lodash");
var inherits = require("util").inherits;
var EventEmitter = require("events").EventEmitter;

function IronStream(projectId, projectToken) {
  if(!projectId || !projectToken) {
    throw new Error("Must include both `projectId` and `projectToken`");
  }
  if(!(this instanceof IronStream)) {
    return new IronStream(projectId, projectToken);
  }

  this.MQ = new IronMQ.Client({project_id: projectId, token: projectToken});
  this.__queues = {};
}

IronStream.prototype.queue = function(name, options) {
  var options = _.merge({
      checkEvery: 1000,
      maxMessagesPerEvent: 10
    }, options);
  this.__queues[name] = this.__queues[name]
    || new Queue(this, name, options);
};


function Queue(ironStream, name, options) {
  this.name = name;
  this.options = options;
  this.__q = ironStream.MQ.queue(name);
  this.messages = [];
}

Queue.prototype.start = function() {
  var me = this;
  var options = {n: this.options.maxMessagesPerEvent};
  if(!this.__stream) {
    this.__stream = setupStream(this);
  };
  this.__i = setInterval(function() {
    me.__q.get(options, function(error, messages) {
      if(error) return me.emit("error", error);
      if(!messages) return;
      messages = _.isArray(messages) ? messages : [messages]; 
      me.messages.concat(messages);
    });
  }, this.options.checkEvery);
};

Queue.prototype.stop = function() {
  clearInterval(this.__i);
};

Queue.prototype.resetMessages = function() {
  this.messages = [];
};


inherits(Queue, EventEmitter);

exports.IronStream = IronStream;
exports.useStub = function(stub) {
  IronMQ = stub;
};

setupStream = function(queue) {
  return through(function write(data) {
    if(queue.messages.length) {
      this.queue(queue.messages);
      queue.resetMessages();
    }
  });
};
