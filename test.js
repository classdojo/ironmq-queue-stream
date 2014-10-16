var expect = require("expect.js"),
Iron = require("./"),
Stub = require("./ironmqstub"),
JsonParser = require("./jsonparser"),
sinon = require("sinon"),
IronStream = Iron.IronStream,
IronSink = Iron.Sink,
stream = require("stream"),
projectId = "SomeProjectId",
projectToken = "SomeProjectToken",
_ = require("lodash");

var config = {
  projectId: projectId,
  projectToken: projectToken
};

describe("IronStream", function() {
  var iron;
  describe("#constructor", function() {
    it("should require a config object with both projectId and projectToken", function(done) {
      try {
        iron = new IronStream(_.omit(config, "projectId"));
      } catch (e) {
        expect(e).to.be.an(Error);
        return done();
      }
      done(new Error("Allowed improper construction"));
    });

    it("should allow construction via invocation", function(done) {
      iron = IronStream(config);
      expect(iron).to.be.an(IronStream);
      done();
    });

    it("should allow construction via `new`", function(done) {
      iron = new IronStream(config);
      expect(iron).to.be.an(IronStream);
      done();
    });
  });

});

var startReading = function(readable) {
  readable._read(); //call once
  var t = setTimeout(readable._read.bind(readable), 500); //simulate getting called every half second
  return {
    stopReading: function() {
      clearTimeout(t);
    }
  }
};

var getQueueMessages = function(queue) {
  return queue.q.dump().messages.concat(
      queue.q.dump().outstandingMessages
  );
};

var CHECK_EVERY = 10;
describe("Queue", function() {
  var iron, queue;
  beforeEach(function() {
    Iron.useStub(Stub);
    iron = IronStream(config);
    queue = iron.queue("someQueue", {checkEvery: CHECK_EVERY, maxMessagesPerEvent: 1});
    queue.q.setMessages([{body: "hello"}, {body: "world"}]);
  });

  describe("#_read", function() {
    var mock, expectation, stub;

    it("starts with this.__flush == false", function() {
      expect(queue.__flush).to.be(false);
    });

    it("sets this.__flush to true when called", function() {
      queue._read();
      expect(queue.__flush).to.be(true);
    });

    it("calls #_startFetching", function() {
      mock = sinon.mock(queue);
      expectation = mock.expects("_startFetching").once();
      queue._read();
      expectation.verify();
      mock.restore();
    });
  });

  describe("#_startFetching", function() {

    it("doesn't set a fetching interval if we're paused", function() {
      queue.running = false;
      queue._startFetching();
      expect(queue.__i).to.not.be.ok();
    });

    it("sets a fetching interval if one does not exist && we're not in a paused state", function() {
      queue._startFetching();
      expect(queue.__i).to.be.ok();
    });

    describe("when __flush == true", function() {
      beforeEach(function() {
        queue.__flush = true;
      });

      it("does not call #_addMessagesToQueue when no messages are returned from IronMQ", function(done) {
        stub = sinon.stub(queue.q, "get");
        stub.yields(null, undefined);

        mock = sinon.mock(queue);
        expectation = mock.expects("_addMessagesToQueue").never();
        queue._startFetching();
        setTimeout(function() {
          expectation.verify();
          mock.restore();
          done();
        }, CHECK_EVERY + 5);
      });

      it("calls #_addMessagesToQueue when messages are returned from IronMQ", function(done) {
        stub = sinon.stub(queue.q, "get");
        stub.yields(null, {body: '{"some": "json"}'});
        mock = sinon.mock(queue);
        expectation = mock.expects("_addMessagesToQueue").once();
        queue._startFetching();
        setTimeout(function() {
          expectation.verify();
          mock.restore();
          done();
        }, CHECK_EVERY + 5);
      });

      it("calls #_startMessageConsumer when messages are returned from IronMQ", function(done) {
        stub = sinon.stub(queue.q, "get");
        stub.yields(null, {body: '{"some": "json"}'});
        mock = sinon.mock(queue);
        expectation = mock.expects("_startMessageConsumer").once();
        queue._startFetching();
        setTimeout(function() {
          expectation.verify();
          mock.restore();
          done();
        }, CHECK_EVERY + 5);
      });
    });

    describe("when __flush == false", function() {

      it("does not call _addMessagesToQueue", function(done) {
        stub = sinon.stub(queue.q, "get");
        stub.yields(null, {body: '{"some": "json"}'});
        mock = sinon.mock(queue);
        expectation = mock.expects("_startMessageConsumer").never();
        queue._startFetching();
        setTimeout(function() {
          expectation.verify();
          mock.restore();
          done();
        }, CHECK_EVERY + 5);
      });

    });

  });

  describe("#_startMessageConsumer", function() {
    var stub;
    beforeEach(function(done) {
      stub = sinon.stub(queue.q, "get");
      stub.onFirstCall().yields(null, {body: '{"some": "json"}'});
      stub.onSecondCall().yields(null, {body: '{"some": "more json"}'});
      //tell queue to put messages in internal buffer
      queue._startFetching();
      setTimeout(done, CHECK_EVERY * 2);
    });

    it("does not start consuming if __consumerInterval is truthy", function(done) {
      queue.__consumerInterval = {};
      queue._startMessageConsumer();
      //let's just give a bit of delay to be sure node I/O loop finishes.
      setTimeout(function() {
        expect(queue.messages).to.have.length(2);
        done();
      }, CHECK_EVERY);
    });

    it("clears __consumerInterval if the messages array is empty", function(done) {
      queue._startMessageConsumer();
      setTimeout(function() {
        expect(queue.__consumerInterval).to.be(null);
        done();
      }, CHECK_EVERY * 3);
    });

    it("clears __consumerInterval and sets __flush == false if this.push signals backpressure", function(done) {
      stub = sinon.stub(queue, "push");
      stub.onFirstCall().returns(false);
      queue._startMessageConsumer();
      setTimeout(function() {
        expect(queue.__consumerInterval).to.be(null);
        expect(queue.messages).to.have.length(1);
        expect(queue.__flush).to.be(false);
        done();
      }, CHECK_EVERY * 3);
    });
  });

  describe("#_addMessagesToQueue", function() {
    var message = {
      body: "some message"
    };
    it("concats message to queue.messages", function() {
      queue._addMessagesToQueue(message);
      expect(queue.messages).to.have.length(1);
    });

    it("concats message to queue.messages if message is an array", function() {
      queue._addMessagesToQueue([message]);
      expect(queue.messages).to.have.length(1);
      expect(queue.messages[0]).to.eql(message);
    });
  });

  describe("#pause", function() {
    it("should stop pulling messages from the queue", function(done) {
      var reader = startReading(queue);
      queue.stopFetching();
      setTimeout(function() {
        expect(queue.q.dump().messages).to.have.length(2);
        reader.stopReading();
        done();
      }, 300);
    });
  });


  describe("#resume", function() {
    var reader;
    beforeEach(function(done) {
      queue.stopFetching();
      reader = startReading(queue);
      setTimeout(done, 100); //wait some time
    });
    it("should resume reading if the queue was previously in a paused state", function(done) {
      queue.resume();
      setTimeout(function() {
        expect(queue.q.dump().messages).to.have.length(0);
        reader.stopReading();
        done();
      }, 300);
    });
  });
});


describe("Sink", function() {
  var sink,
  expectation,
  mock,
  ironQueue;

  beforeEach(function() {
    Iron.useStub(Stub);
    var iron = new IronStream(config);
    ironQueue = iron.queue("someQueue", {checkEvery: 100, maxMessagesPerEvent: 1});
  });

  it("should emit a deleteError event if the message does not have an id field", function() {
    var badMessage = {message: "without id"};
    sink = new IronSink(ironQueue);
    mock = sinon.mock(sink);
    expectation = mock
                    .expects("emit")
                    .once()
                    .withArgs("deleteError", new Error("Message does not have an `id` property"), badMessage);
    sink._write(badMessage, "utf-8", function() {});
    expectation.verify();
    mock.restore();
  });

  it("it should call delete on the queue when a job contains an id", function() {
    var message = {id: "123", message: "some Message"};
    mock = sinon.mock(ironQueue.q); //stub the internal queue lib.
    expectation = mock
                    .expects("del")
                    .once()
                    .withArgs("123");
    sink = new IronSink(ironQueue);
    sink._write(message, "utf-8", function() {});
    expectation.verify();
    mock.restore();
  });
});


//helpers used with JsonParser tests
var suppressErrors = function(sut) {
  sut.on("error", function() {});
};
var suppressStreamPush = function(sut) {
  sinon.stub(sut, "push")
    .returns(true);
};

describe("JsonParser", function() {
  var jsonParser,
      expectedData,
      jsonParserMock,
      expectation;


  var testJson = {
    goodJson1:  '{"service": "someService1", "action": "someAction", "payload": "somePayload"}',
    goodJson2: '{"service": "someService2", "action": "someAction", "payload": {}}',
    badJson: '{"hello": }',
    jsonWithNestedJson: {"_id": "1", "body": '{"service": "someService", "action": "someAction", "payload": "somePayload"}' }
  };


  beforeEach(function() {
    jsonParser = new JsonParser();
    jsonParserMock = sinon.mock(jsonParser);
  });
  afterEach(function() {
    jsonParserMock.restore();
  });

  it("should decode an array of jobs into JSON when it receives it", function() {
    expectedData = [JSON.parse(testJson.goodJson1), JSON.parse(testJson.goodJson2)];
    expectation = jsonParserMock
                        .expects("push")
                        .once()
                        .withArgs(expectedData);
    jsonParser._transform(
        [testJson.goodJson1, testJson.goodJson2],
        "utf-8",
        function() {}
    );
    expectation.verify();
  });

  it("should decode a single job into JSON", function() {
    expectedData = JSON.parse(testJson.goodJson1);
    expectation = jsonParserMock
                        .expects("push")
                        .once()
                        .withArgs(expectedData);
    jsonParser._transform(testJson.goodJson1, "utf-8", function() {});
    expectation.verify();
  });

  it("should emit a parseError when it receives bad json", function() {
    expectation = jsonParserMock
                    .expects("emit")
                    .once()
                    .withArgs("parseError", new Error('SyntaxError: Unexpected token }'), testJson.badJson);
    jsonParser._transform(
        [testJson.badJson],
        "utf-8",
        function() {}
    );
    expectation.verify();
  });

  it("should pass through the good jobs in a batch when at least one is bad", function() {
    expectedData = [JSON.parse(testJson.goodJson1), JSON.parse(testJson.goodJson2)];
    expectation = jsonParserMock
                    .expects("push")
                    .once()
                    .withArgs(expectedData);
    suppressErrors(jsonParser);
    jsonParser._transform(
        [testJson.goodJson1, testJson.goodJson2, testJson.badJson],
        "utf-8",
        function() {}
     );
    expectation.verify();
  });

  describe("`opts`", function() {
    var opts;
    describe("parseField", function(done) {

      it("should emit a parseError when the field specified is not defined in the input object", function() {
        opts = {parseField: "someField"};
        jsonParser = new JsonParser(opts);
        expectation = sinon.mock(jsonParser)
                          .expects("emit")
                          .once()
                          .withArgs("parseError", new Error("Object does not contain field someField"), testJson.jsonWithNestedJson);
        jsonParser._transform(
            [testJson.jsonWithNestedJson],
            "utf-8",
            function() {}
        );
        expectation.verify();
      });

      it("should push the parsed object", function() {
        opts = {parseField: "body"};
        jsonParser = new JsonParser(opts);
        expectation = sinon.mock(jsonParser)
                          .expects("push")
                          .once()
                          .withArgs([JSON.parse(testJson.jsonWithNestedJson.body)])
        jsonParser._transform(
            [testJson.jsonWithNestedJson],
            "utf-8",
            function() {}
        );
        expectation.verify();
      });
    });

    describe("enrichWith", function() {
      var opts;
      it("should enrich the parsed object with a field from the original object if a valid path is specified", function() {
        opts = {parseField: "body", enrichWith: ["_id"]};
        jsonParser = new JsonParser(opts);
        expectation = sinon.mock(jsonParser)
                          .expects("push")
                          .once()
                          .withArgs([
                            _.merge(JSON.parse(testJson.jsonWithNestedJson.body),
                                    {_id: testJson.jsonWithNestedJson._id})
                          ]);
        jsonParser._transform(
            [testJson.jsonWithNestedJson],
            "utf-8",
            function() {}
        );
        expectation.verify()
      });

      it("should enrich the object with a new object if an object is specified", function() {
        opts = {parseField: "body", enrichWith: [{_id: testJson.jsonWithNestedJson._id}]};
        jsonParser = new JsonParser(opts);
        expectation = sinon.mock(jsonParser)
                          .expects("push")
                          .once()
                          .withArgs([
                            _.merge(JSON.parse(testJson.jsonWithNestedJson.body),
                                    {_id: testJson.jsonWithNestedJson._id})
                          ]);
        jsonParser._transform(
            [testJson.jsonWithNestedJson],
            "utf-8",
            function() {}
        );
        expectation.verify();
      });
    });
  });
});
