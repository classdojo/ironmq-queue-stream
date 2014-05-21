var expect = require("expect.js");
var IronStream = require("./").IronStream;
var Stub = require("./ironmqstub");

describe("IronStream", function() {
  var projectId = "SomeProjectId";
  var projectToken = "SomeProjectToken";
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

  describe("#queue", function() {
    before(function() {
      iron = new IronStream(projectId, projectToken);
    });

  });
});

describe("Queue", function() {
  describe("#_read", function() {
  });

  describe("#resume", function() {
  });

  describe("#stop", function() {
  });

});
