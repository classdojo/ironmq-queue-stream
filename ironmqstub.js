/* 

  Stub that captures the data sent to ironmq. The
  functionality is taken from the official IronMQ node
  client found here: https://github.com/iron-io/iron_mq_node
  
  version 0.2.0
*/
var _ = require("lodash");

var RELEASE_TIMEOUT = 5000;


function Client (opts) {
  this.__opts = opts;
  this.__queues = {};
}

Client.prototype.queue = function(queueName, releaseTimeout) {
  if(!queueName) {
    throw new Error("Must provide a valid Queue name. It cannot be `undefined`");
  }
  if (this.__queues[queueName]) {
    return this.__queues[queueName];
  }
  var q = new Queue(queueName, releaseTimeout);
  this.__queues[queueName] = q;
  return q;
}

function Queue(name, releaseTimeout){
  this.name = name;
  this.messages = [];
  this.outstandingMessages = [];
  this.__timeouts = {};
  this.releaseTimeout = releaseTimeout || RELEASE_TIMEOUT;
}

/**
 * Post an object to the IronMQ queue.
 * 
 * Examples:
 *
 *  queue.post("Hello World!", function(err){
 *
 *  });
 *   
 * queue.post({body: "Hello World!"}, function(err) {
 *
 *  });
 *
 *  queue.post(
 *    [{body: "Hello World1!"},
 *     {body: "Hello World2!"}],
 *    function(err) {}
 *  );
 *
 * @param {String|Object|Array} data
 * @param {Function} cb
 */

Queue.prototype.post = function(data, cb) {
  if (_.isArray(data)) {
    //map through body and push onto data.
    for (item in data) {
      _pushMessage(this, data[item]);
    }
  } else if (_.isString(data)) {
    _pushMessage(this, data);
  } else if (_.isObject(data)) {
    _pushMessage(this, data);
  } else {
    return cb(new Error("Invalid Type"));
  }
  cb(null); //always success
}

/**
 * Get N objects from the IronMQ queue.
 * 
 * Examples:
 *
 * queue.get({}, function(err, body){
 *    //body is 1 message object
 * });
 *   
 * queue.get({n: 5}, function(err, body) {
 *   //body is an array of message objects
 * });
 *
 * An example of a body object is:
 *
 * {
 *   "type": "mailgun:update_list",
 *   "data": {
 *     // Object|String included depending on data `post`ed to queue.
 *   },
 *   "created": "'2013-11-02T00:30:17.765Z'",
 *   "attempts": 0
 * }
 *
 * If there are no messages on the queue [] is passed to the callback if n > 1, else
 * `undefined` is passed back.
 *
 * @param {Object} options
 * @param {Function} cb
 */
Queue.prototype.get = function(options, cb) {
  var BaseId = 5940635112690560000;
  var random = 1000 + Math.floor(Math.random() * 100);
  var value;
  var times;
  if (options && options.n > 1) {
    //create array
    if (_.isEmpty(this.messages)) {
      return cb(null, []);
    }
    var me = this;
    if(options.n <= this.messages.length) {
      times = options.n;
    } else {
      times = this.messages.length;
    }
    value = _.times(times, function() {
      return _popMessage(me);
    });
  }
  else {
    value = _popMessage(this);
  }
  cb(null, value);
}

/*
  Mock del method since when a client gets the message from this
  stub we simply delete it from the queue.
*/
Queue.prototype.del = function(id, cb) {
  var error;
  if (!_.isString(id)) {
    throw new Error("`id` must be a string");
  } else if (!_.isFunction(cb)) {
    throw new Error("`cb` must be a function");
  }
  if(_findAndRemove(id, this.messages, this.outstandingMessages)) {
    cb(null, {msg: "Deleted"});
  } else {
    error = new Error("The message is not in the queue. It's likely that your job took longer than #{RELEASE_TIMEOUT} to run");
    cb(error);
  }
}


/*
  Mock msg_release.

  Note: method doesn't put the message back on the queue.
*/
Queue.prototype.msg_release = function(id, options, cb) {
  //just always succeed on release
  if (!_.isString(id)) {
    throw new Error("`id` must be a string");
  } else if (!_.isObject(options)) {
    throw new Error("`options` must be an object");
  } else if (!_.isFunction(cb)) {
    throw new Error("`cb` must be a function");
  }
  var messageToRelease = _findAndRemove(id, this.outstandingMessages);
  if(messageToRelease) {
    this.messages.push(messageToRelease);
  }
  if(this.__timeouts[id]) {
    clearTimeout(this.__timeouts[id]);
    delete(this.__timeouts[id]);
  }
  cb(null, {msg: "Released"});
}

Queue.prototype.clear = function() {
  this.messages = [];
  this.outstandingMessages = [];
  for(var property in this.__timeouts) {
    clearTimeout(this.__timeouts[property]);
  }
  this.__timeouts = {};
}

/*
  Test method.  Sets the Queue
*/
Queue.prototype.setMessages = function(messages) {
  this.clear();
  if(!_.isArray(messages)) {
    throw new Error("Must pass in an error");
  }
  for ( message in messages ) {
    _pushMessage(this, messages[message]);
  }
}

Queue.prototype.dump = function() {
  return { 
    messages: this.messages,
    outstandingMessages: this.outstandingMessages
  };
}

exports.Client = Client;
exports.Queue  = Queue;

/*
  private fns
*/

var _popMessage = function(queue) {
  var message = queue.messages.splice(0,1)[0];
  if(message) {
    queue.outstandingMessages.push(message);
  }
  //put back on messages queue after a timeout.
  if(message) {
    var timeout = setTimeout(function() {
      var index = _.findIndex(queue.outstandingMessages, function(e) {return e.id == message.id});
      if(index != -1) {
        var messageToRelease = _findAndRemove(message.id, queue.outstandingMessages);
        if(messageToRelease) {
          queue.messages.push(messageToRelease);
        }
      } else{ 
        //no-op
      }
    }, queue.releaseTimeout);
    queue.__timeouts[message.id] = timeout;
  }
  return _.cloneDeep(message);
}


/*
  A message always gets put on the queue as a stringified body.
*/
var inc = 0;
var _pushMessage = function(queue, message) {
  var BaseId, random, id;
  if (_.isObject(message)) {
    //must have body: param.
    if (message.body) {
      //if is object stringify and pass on.
      if(_.isString(message.body)) {
        message = message.body;
      } else if (_.isObject(message.body)) {
        message = JSON.stringify(message.body);
      } else { throw new Error("Invalid message type.")}
    }
  }
  var defaultResponse = {
    timeout: 60,
    reserved_count: 3,
    push_status: {}
  };
  BaseId = "5940635112690500000";
  random = String(inc);
  BaseId = BaseId.slice(0, BaseId.length - random.length - 1)
  id = BaseId + random;
  queue.messages.push(_.extend(defaultResponse, {id: id, body: message}));
  inc++;
}

// signature function(id [arrays])
var _findAndRemove = function () {
  var arrays = _.toArray(arguments);
  var id = arrays.shift();
  for (i in arrays) {
    var arr = arrays[i];
    var index = _.findIndex(arr, function(e) {return e.id == id});
    if(index != -1) {
      var m = arr.splice(index, 1);
      return m.shift();
    }
  }
  return false;
}
