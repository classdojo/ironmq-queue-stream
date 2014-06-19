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

describe("IronStream", function() {
  var iron;
  describe("#constructor", function() {
    it("should require an arity of two", function(done) {
      try {
        iron = new IronStream("");
      } catch (e) {
        expect(e).to.be.an(Error);
        return done();
      }
      done(new Error("Allowed improper construction"));
    });

    it("should allow construction via invocation", function(done) {
      iron = IronStream(projectId, projectToken);
      expect(iron).to.be.an(IronStream);
      done();
    });

    it("should allow construction via `new`", function(done) {
      iron = new IronStream(projectId, projectToken);
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
describe("Queue", function() {
  var iron, queue;
  beforeEach(function() {
    Iron.useStub(Stub);
    iron = IronStream(projectId, projectToken);
    queue = iron.queue("someQueue", {checkEvery: 100, maxMessagesPerEvent: 1});
    queue.q.setMessages([{body: "hello"}, {body: "world"}]);
  });

  it("should pull messages off the queue at the specified interval", function(done) {
    //pump data by hooking up to a writable stream
    var reader = startReading(queue);
    //manually inspect the underlying queue to see how many messages are on there.
    setTimeout(function() {
      expect(queue.q.dump().messages).to.have.length(0);
      reader.stopReading();
      done();
    }, 300);
  });

  describe("#pause", function() {
    it("should stop pulling messages from the queue", function(done) {
      var reader = startReading(queue);
      queue.pause();
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
      queue.pause();
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
    var iron = new IronStream(projectId, projectToken);
    ironQueue = iron.queue("someQueue", {checkEvery: 100, maxMessagesPerEvent: 1});
  });

  it("should emit an error if the message does not have an id field", function() {
    var badMessage = {message: "without id"};
    sink = new IronSink(ironQueue);
    mock = sinon.mock(sink);
    expectation = mock
                    .expects("emit")
                    .once()
                    .withArgs("error", new Error("Message does not have an `id` property"), badMessage);
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

  it("should decode a job into JSON when it receives it", function() {
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

  it("should emit an error when it receives bad json", function() {
    expectation = jsonParserMock
                    .expects("emit")
                    .once()
                    .withArgs("error", new Error('SyntaxError: Unexpected token }'), testJson.badJson);
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
  });

  describe("`opts`", function() {
    var opts;
    describe("parseField", function(done) {

      it("should emit an error when the field specified is not defined in the input object", function() {
        opts = {parseField: "someField"};
        jsonParser = new JsonParser(opts);
        expectation = sinon.mock(jsonParser)
                          .expects("emit")
                          .once()
                          .withArgs("error", new Error("Object does not contain field someField"), testJson.jsonWithNestedJson);
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
      });
    });
  });
});
