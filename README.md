# ironmq-queue-stream

## Tests

```bash
make test
```


## Usage
```javascript
IronStream = require("ironmq-queue-stream");

var iron = new IronStream("projectId", "projectToken");

//initialize a queue for that stream to pull from
iron.queue("someMessageQueue");

//pipe that stream to something useful
iron.pipe(someOtherStream);

/*
  The output of iron stream is the raw json pulled from the iron queue.
  Sometimes you want to reliably parse json that you stored on the queue
  to then process downstream.

  Say we stored a stringified json object on the queue like: 
    '{
      "some": "message"
    }'

  What's actually output from the ironmq queue is
    {
      "id": "123",
      "body": '{"some": "message"}'
    }

  Ironmq Stream provides a helper method to output a parsed json object, allowing
  the client to define an optional onError handler that executes when there's some
  parsing error.
*/

var parsedStream = IronStream.parseJson(iron, function(err, message) {
  console.error(err + " for message " + message);
});

parsedStream.pipe(someOtherStream);
/*
  someOtherStream now gets
  {
    "id": "123",
    "some": "message"
  }
  The object is enriched with id in case you want to use a Queue sink downstream.
*/

```

### Sinks

```javascript
/*
  Sometimes you might want to delete a message off IronMQ after doing some processing.
  Sinks make that easy.
*/
var Sink = require("iron-stream").Sink;
var iron = new IronStream("projectId", "projectToken");
//this time let's grab a reference to the queue object returned from .queue
var myQueue = iron.queue("myQueue");
sink = new Sink(myQueue);
iron.pipe(someOtherStream).pipe(sink); //every successful message is deleted from the queue.
```
