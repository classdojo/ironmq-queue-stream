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

describe("Fetcher", function() {
  describe("#start", function() {
    it("sets this.running to true");

    it("calls fetch at the given interval");

    it("emits an `error` event if fetch returns an error");

    it("emits a `results` event if fetch returns results");

  });

  describe("#stop", function() {
    it("sets this.running to false");

    it("clears __i interval if defined");

    it("nullifies __i if defined");
  });

  describe("#shutdown", function() {
    it("sets this.shuttingDown");

    it("calls this.stop");
  });
})

var CHECK_EVERY = 10;
describe("Queue", function() {
  var iron, queue;
  beforeEach(function() {
    Iron.useStub(Stub);
    iron = IronStream(config);
    queue = iron.queue("someQueue", {checkEvery: CHECK_EVERY, maxMessagesPerEvent: 1});
    queue.q.setMessages([{body: "hello"}, {body: "world"}]);
  });

  it("initializes a fetcher");

  it("adds a default results handler to fetcher");

  it("adds a default error handler to fetcher");

  describe("#firstMessageListener", function() {
    it("calls _pushOneMessage() when results are not empty");

    it("calls fetcher#stop if only one result is passed in");

    it("does not call fetcher#stop if more than one result is passed in");
  });

  describe("#_read", function() {
    var mock, expectation, stub;

    describe("Empty messages", function() {

      it("calls fetcher start when fetcher is not running");

      it("adds a one time results listener to add the first message to the queue");

    });
    describe("Non-empty messages", function() {

      it("stops the fetcher when fetcher.running == true");

      it("pushes one message downstream");

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
    it("sets this.running = true", function() {
      queue.resume();
      expect(queue.running).to.be(true);
    });
  });
});


describe("Sink", function() {
  var sink,
  expectation,
  mock,
  ironQueue,
  stub;

  beforeEach(function() {
    Iron.useStub(Stub);
    var iron = new IronStream(config);
    ironQueue = iron.queue("someQueue", {checkEvery: 100, maxMessagesPerEvent: 1});
  });

  it("emits a deleteError event if the message does not have an id field", function() {
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

  it("xxx emits a delete event if the message is properly deleted", function(done) {
    var message = {id: "123", message: "some Message"};

    sink = new IronSink(ironQueue);
    stub = sinon.stub(sink.q, "del")
              .onFirstCall()
              .yields(null);

    mock = sinon.mock(sink);
    expectation = mock
                    .expects("emit")
                    .once()
                    .withArgs("deleted");
    sink._write(message, "utf-8", function() {
      expectation.verify();
      mock.restore();
      done();
    });
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
