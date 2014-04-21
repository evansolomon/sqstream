# SqStream

```js
var SqStream = require('sqstream')
var AWS = require('aws-sdk')
var sqs = new AWS.SQS()

var sqStream = new SqStream('some-queue', sqs)

// Send data to SQS
streamOfMessages.pipe(sqStream)

// Get data from SQS
sqStream.on('data', function (message) {
  console.log(message.Body)
})
```
