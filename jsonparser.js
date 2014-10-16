var Transform = require("stream").Transform,
inherits = require("util").inherits,
dref = require("dref"),
_ = require("lodash");

inherits(JsonParser, Transform);


/*
  NOTE: Instead of this I'd like to use a solution like JSONStream 
  (https://github.com/dominictarr/JSONStream).  The problem is I require
  that the resulting parsed json be enriched with other fields, possibly from the root object, which
  JSONStream does not give you access to in it's map function. Open to suggestions to deprecate this
  code.


  @param opts {Object}
    opts.parseField {String} A dot separated path specifying the value to run through JSON.parse()

    opts.enrichWith {Array of Strings or Objects}
      Strings - dot separated paths into an object. Value is set at the top level of the new object under the
                key specified by the String
      Object - simply merged into parsed object at the top level.

*/
function JsonParser(opts) {
  //use simple through function
  this.opts = opts || {};
  Transform.call(this, {objectMode: true, decodeStrings: false});
}

/*
 * _transform
 *
 * @param jobs {Array}
*/

JsonParser.prototype._transform = function(jobs, enc, cb) {
  var decodedJobs = [],
  toParse, parsed, enrichField, currentJob;
  var isArray = _.isArray(jobs);
  if(!isArray) {
    jobs = [jobs]
  }
  for(var i = 0; i < jobs.length; i++) {
    //pluck out field under test
    currentJob = jobs[i];
    toParse = this.opts.parseField ? dref.get(currentJob, this.opts.parseField)
                                       : currentJob;
    if(!toParse) {
      return this.emit("parseError", new Error("Object does not contain field", this.opts.parseField), currentJob);
    }
    try {
      parsed = JSON.parse(toParse)
    } catch (e) {
      //emit to error channel
      this.emit("parseError", e, currentJob);
      continue;
    }
    if(this.opts.enrichWith) {
      //enrich the object
      for(var i = 0; i < this.opts.enrichWith.length; i++) {
        enrichField = this.opts.enrichWith[i];
        if(_.isString(enrichField)) {
          dref.set(parsed, enrichField, dref.get(currentJob, enrichField));
        } else if(_.isObject(enrichField)) {
          parsed = _.merge(parsed, this.opts.enrichWith[i]);
        }
      }
    }
    decodedJobs.push(parsed);
  }
  if(decodedJobs.length) {
    if(isArray) {
      this.push(decodedJobs);

    } else {
      this.push(decodedJobs.shift());
    }
  }
  cb();
};

module.exports = JsonParser;
