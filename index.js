var Duplex = require('stream').Duplex
var util = require('util')
var Q = require('q')

util.inherits(SqStream, Duplex)
module.exports = SqStream

/**
 * @param {string} queueName Name of your SQS queue
 * @param {AWS.SQS} sqs Instance of AWS.SQS
 */
function SqStream(queueName, sqs) {
  Duplex.call(this, {
    objectMode: true,
    decodeStrings: false,
    allowHalfOpen: true
  })

  this._queueName = queueName
  this._sqs = sqs

  // SQS constraints
  this.maxMessageSize = 262144
  this.maxMessagesPerSend = 10
  this.maxMessagesPerGet = 10
  this.visibilityTimeout = 30

  // Status
  this._queueUrl = null
  this._inFlightAcks = 0
  this._waitingToEnd = false
  this._pendingWrites = []
  this._sendingMessages = false

  this.on('prefinish', this.processWrites.bind(this))
}

/**
 * Get the URL for the named SQS queue. Cached after the first request.
 * @return {Promise}
 */
SqStream.prototype.getUrl = function () {
  var deferred = Q.defer()

  setImmediate(function () {
    if (this._queueUrl) {
      return deferred.resolve(this._queueUrl)
    }

    this._sqs.getQueueUrl({
      QueueName: this._queueName
    }, function (err, data) {
      if (err) {
        return deferred.reject(err)
      }

      this._queueUrl = data.QueueUrl
      deferred.resolve(data.QueueUrl)
    }.bind(this))
  }.bind(this))

  return deferred.promise
}

/**
 * Retreive a batch of messages from SQS.
 * @return {Promise}
 */
SqStream.prototype.getMessages = function () {
  return this.getUrl()
    .then(function (url) {
      return Q.ninvoke(this._sqs, 'receiveMessage', {
        QueueUrl: url,
        MaxNumberOfMessages: this.maxMessagesPerGet,
        VisibilityTimeout: this.visibilityTimeout
      })
    }.bind(this))
    .then(function (data) {
      return data.Messages
    })
}

/**
 * Delete messaages from SQS.
 * @param  {array} messages
 * @return {Promise}
 */
SqStream.prototype.deleteMessages = function (messages) {
  var deferred = Q.defer()

  setImmediate(function () {
    var entries = messages.map(function (message) {
      return {
        Id: message.MessageId,
        ReceiptHandle: message.ReceiptHandle
      }
    })

    if (entries.length === 0) {
      return deferred.resolve()
    }

    this.getUrl()
      .then(function (url) {
        this._sqs.deleteMessageBatch({
          Entries: entries,
          QueueUrl: url
        }, deferred.makeNodeResolver())
      }.bind(this))
      .fail(deferred.reject)
  }.bind(this))

  return deferred.promise
}

/**
 * Push messages to consumers of this stream.
 * @param {array?} messages
 * @return {Promise}
 */
SqStream.prototype.handleMessages = function (messages) {
  var deferred = Q.defer()

  if (this._waitingToEnd) {
    setImmediate(deferred.resolve.bind(null, false))
  } else if (! messages) {
    this._waitingToEnd = true
    var timer = setInterval(function () {
      if (this._inFlightAcks > 0) return

      clearInterval(timer)
      this.push(null)
      deferred.resolve(false)
    }.bind(this))
  } else {
    this._inFlightAcks++
    this.deleteMessages(messages)
      .then(function () {
        messages.forEach(this.push.bind(this))
      }.bind(this))
      .fail(deferred.reject)
      .fin(function () {
        this._inFlightAcks--
      }.bind(this))
  }

  return deferred.promise
}

/**
 * Implement _read method for duplex stream.
 */
SqStream.prototype._read = function () {
  this.getMessages().then(this.handleMessages.bind(this))
}

/**
 * Send pending messages to SQS
 */
SqStream.prototype.processWrites = function () {
  if (this._pendingWrites.length === 0) return

  this._sendingMessages = true
  var pendingWrites = this._pendingWrites.slice(0)
  this._pendingWrites.length = 0

  var entries = pendingWrites.reduce(function (memo, message, idx) {
    memo.push({
      Id: idx.toString(),
      MessageBody: message
    })

    return memo
  }, [])

  this.getUrl().then(function (url) {
    this._sqs.sendMessageBatch({
      QueueUrl: url,
      Entries: entries
    }, function (err) {
      this._sendingMessages = false
      if (err) {
        this.emit('error', err)
      }
    }.bind(this))
  }.bind(this))
}

SqStream.prototype.queueWrite = function (message, callback) {
  callback()
  this._pendingWrites.push(message)
  if (! this.readyForWrite()) {
    this.processWrites()
  }
}

SqStream.prototype.readyForWrite = function (message) {
  if (this._pendingWrites.length >= this.maxMessagesPerSend) {
    return false
  }

  if (this._sendingMessages) {
    return false
  }

  var size = this._pendingWrites.reduce(function (sum, message) {
    return sum + Buffer.byteLength(message)
  }, message ? Buffer.byteLength(message) : 0)

  if (size + size >= this.maxMessageSize) {
    return false
  }

  return true
}

/**
 * Implement _write method for duplex stream.
 * @param {*} message
 * @param {string} encoding Ignored.
 * @param  {function} callback
 */
SqStream.prototype._write = function (message, encoding, callback) {
  var isString = typeof message === 'string'
  if (Buffer.isBuffer(message)) {
    message = message.toString()
  } else if (! isString && Object(message) === message) {
    message = JSON.stringify(message)
  } else if (! isString) {
    message = message.toString()
  }

  if (this.readyForWrite(message)) {
    this.queueWrite(message, callback)
  } else {
    var timer = setInterval(function () {
      if (this.readyForWrite()) {
        clearInterval(timer)
        this.queueWrite(message, callback)
      }
    }.bind(this), 500)
  }
}
