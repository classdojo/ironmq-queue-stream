# ironmq-stream (WIP)


## Tests

```bash
make test
```



```javascript
IronStream = require("ironmq-stream");

var iron = new IronStream("projectId", "projectToken");


var test = iron.queue("test", {checkEvery: 10, maxMessagesPerEvent: 100});

test.on("readable", function() {
    console.log("You've got messages.", this.read());
});

test.on('error', function(err) {
  //handle fetch error
});
```

test.start();
