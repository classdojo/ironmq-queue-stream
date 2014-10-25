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
  var fetcher;

  beforeEach(function() {
    fetcher = new Iron.Fetcher();
  });
  describe("#start", function() {
  });

  describe("#_fetch", function() {
  });

  describe("#stop", function() {
  });

  describe("#shutdown", function() {
  });
})

var CHECK_EVERY = 10;
describe("Queue", function() {
  var iron, queue;
  beforeEach(function() {
    Iron.useStub(Stub);
    iron = IronStream(config);
    queue = iron.queue("someQueue", {ironmq: {}, stream: {}});
    queue.q.setMessages([{body: "hello"}, {body: "world"}]);
  });

  it("initializes a fetcher", function() {
    expect(queue.fetcher).to.be.a(Iron.Fetcher);
  });


  describe("receiving messages", function() {
    var spy;

    var sendMessages = function(messages) {
      queue.fetcher.emit("results", messages);
    };

    it("adds the messages to the internal queue");

    describe("when the first request with results is received", function() {
      it("pushes the first result downstream");

      it("does not stop the fetcher if the internal message queue is empty after the first push");

      it("stops the fetcher if the internal message queue is not empty after the first push");
    });
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
    ironQueue = iron.queue("someQueue", {ironmq: {}, stream: {}});
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

  it("it should call delete on the queue when a job contains an id");

  it("emits a delete event if the message is properly deleted");

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
