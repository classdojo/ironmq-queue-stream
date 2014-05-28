var expect = require("expect.js"),
Iron = require("./"),
Stub = require("./ironmqstub"),
IronStream = Iron.IronStream,
stream = require("stream"),
projectId = "SomeProjectId",
projectToken = "SomeProjectToken";

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
  return queue.__q.dump().messages.concat(
      queue.__q.dump().outstandingMessages
  );
};
describe("Queue", function() {
  var iron, queue;
  beforeEach(function() {
    Iron.useStub(Stub);
    iron = IronStream(projectId, projectToken);
    queue = iron.queue("someQueue", {checkEvery: 100, maxMessagesPerEvent: 1});
    queue.__q.setMessages([{body: "hello"}, {body: "world"}]);
  });

  it("should pull messages off the queue at the specified interval", function(done) {
    //pump data by hooking up to a writable stream
    var reader = startReading(queue);
    //manually inspect the underlying queue to see how many messages are on there.
    setTimeout(function() {
      expect(queue.__q.dump().messages).to.have.length(0);
      reader.stopReading();
      done();
    }, 300);
  });

  describe("#pause", function() {
    it("should stop pulling messages from the queue", function(done) {
      var reader = startReading(queue);
      queue.pause();
      setTimeout(function() {
        expect(queue.__q.dump().messages).to.have.length(2);
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
        expect(queue.__q.dump().messages).to.have.length(0);
        reader.stopReading();
        done();
      }, 300);
    });
  });
});
