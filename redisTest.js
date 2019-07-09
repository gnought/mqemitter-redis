'use strict'

function noop () {}

function buildTests (opts) {
  var builder = opts.builder
  var isCluster = opts.isCluster
  var test = opts.test

  test('actual unsubscribe from Redis', function (t) {
    var e = builder()

    e.subConn.on('message', function (topic, message) {
      t.fail('the message should not be emitted')
    })

    e.on('hello', noop)
    e.removeListener('hello', noop)
    e.emit({ topic: 'hello' }, function () {
      e.close(function () {
        t.end()
      })
    })
  })

  test('same as mqemitter behaviour', function (t) {
    var e = builder()
    var count1 = 0
    var count2 = 0
    e.on('hello', function (message, cb) {
      if (message.payload === 'world') {
        count1++
      }
      if (message.payload === 'foo') {
        count2++
      }
      cb()
    })
    e.on('hello', function (message, cb) {
      if (message.payload === 'world') {
        count1++
      }
      if (message.payload === 'foo') {
        count2++
      }
      cb()
    })
    e.emit({ topic: 'hello', payload: 'world' }, noop)
    e.on('hello', function (message, cb) {
      if (message.payload === 'world') {
        count1++
      }
      if (message.payload === 'foo') {
        count2++
      }
      cb()
    })
    e.emit({ topic: 'hello', payload: 'foo' }, function () {
      setTimeout(() => {
        e.close(function () {
          t.equal(count1, 2)
          t.equal(count2, 3)
          t.end()
        })
      }, 200)
    })
  })

  test('ioredis removelistener', function (t) {
    t.plan(1)

    var e = builder()
    var count = 0
    var beCalled = function (message, cb) {
      count++
      cb()
    }
    var topic = 'hello/world'
    e.on('hello/+', beCalled)
    e.removeListener('hello/+', beCalled)
    e.emit({ topic: topic }, () => {
      setTimeout(() => {
        e.close(function () {
          t.equal(count, 0)
          t.end()
        })
      }, 200)
    })
  })

  test('ioredis removelistener in multiple subscriptions', function (t) {
    t.plan(1)

    var e = builder()
    var count = 0
    var beCalled1 = function (message, cb) {
      count++
      cb()
    }
    var beCalled2 = function (message, cb) {
      count++
      cb()
    }
    var topic = 'hello'
    e.on(topic, beCalled1)
    e.on(topic, beCalled2)
    e.emit({ topic: topic }, noop)
    e.removeListener(topic, beCalled1)
    e.emit({ topic: topic }, () => {
      setTimeout(() => {
        e.close(function () {
          t.equal(count, 3)
          t.end()
        })
      }, 200)
    })
  })

  test('ioredis removelistener wildcard topic in multiple subscriptions', function (t) {
    t.plan(1)

    var e = builder()
    var count = 0
    var beCalled1 = function (message, cb) {
      count++
      cb()
    }
    var beCalled2 = function (message, cb) {
      count++
      cb()
    }
    var topic = 'hello/world'
    e.on('hello/+', beCalled1)
    e.on('hello/+', beCalled2)
    e.emit({ topic: topic }, noop)
    e.removeListener('hello/+', beCalled1)
    e.emit({ topic: topic }, () => {
      setTimeout(() => {
        e.close(function () {
          t.equal(count, 3)
          t.end()
        })
      }, 200)
    })
  })

  test('removelistener a right one', function (t) {
    t.plan(3)

    var e = builder()

    var notBeCalled = function (message, cb) {
      t.fail('the message should not be emitted')
      cb()
    }
    var beCalled = function (message, cb) {
      t.ok('message received')
      cb()
    }
    var topic = 'hello'
    e.on(topic, notBeCalled)
    e.on(topic, beCalled)
    t.equal(e._matcher.match(topic).length, 2, 'should be 2')
    e.removeListener(topic, notBeCalled)
    t.equal(e._matcher.match(topic).length, 1, 'should be 1')
    e.emit({ topic: topic }, () => {
      e.close(function () {
        t.end()
      })
    })
  })

  test('topic is in Unicode', function (t) {
    t.plan(3)

    var e = builder()

    var topic = '👌😎'
    var notBeCalled = function (message, cb) {
      t.ok('message received')
      t.equal(message.topic, topic)
      t.equal(message.payload, topic)
      cb()
    }
    e.on(topic, notBeCalled)
    e.emit({ topic: topic, payload: topic }, () => {
      e.close(function () {
        t.end()
      })
    })
  })

  test('double close', function (t) {
    t.plan(1)

    var e = builder()
    e.close()
    e.close(() => {
      t.fail('should not reach here')
    })
    t.ok(e.closed)
    t.end()
  })

  // test('multiple mqemitter-redis can share a redis', function (t) {
  //   t.plan(2)

  //   var e1 = builder()
  //   var e2 = builder()
  //   var count = 0

  //   e1.on('hello', function (message, cb) {
  //     t.ok(message, 'message received')
  //     cb()
  //   }, () => { count++; newEvent() })
  //   e2.on('hello', function (message, cb) {
  //     t.ok(message, 'message received')
  //     e2.close()
  //     cb()
  //   }, () => { count++; newEvent() })

  //   function newEvent () {
  //     if (count === 2) {
  //       e1.emit({ topic: 'hello' }, function () {
  //         e1.close()
  //       })
  //     }
  //   }
  // })

  // test('unsubscribe one in multiple mqemitter-redis in a shared redis', function (t) {
  //   t.plan(1)

  //   var e1 = builder()
  //   var e2 = builder()
  //   var count = 0

  //   e1.on('hello', noop, () => { count++; newEvent() })
  //   e1.subConn.on('message', function (topic, message) {
  //     if (topic) {
  //       t.fail('the message should not be emitted')
  //     }
  //   })
  //   e2.on('hello', function (message, cb) {
  //     t.ok(message, 'message received')
  //     cb()
  //   }, () => { count++; newEvent() })
  //   function newEvent () {
  //     if (count === 2) {
  //       e1.removeListener('hello', noop, () => {
  //         e2.emit({ topic: 'hello' }, function () {
  //           e2.close()
  //           e1.close(function () {
  //             t.end()
  //           })
  //         })
  //       })
  //     }
  //   }
  // })

  test('ioredis connect event', function (t) {
    var e = builder()

    var subConnectEventReceived = false
    var pubConnectEventReceived = false

    e.state.on('pubConnect', function () {
      pubConnectEventReceived = true
      newConnectionEvent()
    })

    e.state.on('subConnect', function () {
      subConnectEventReceived = true
      newConnectionEvent()
    })

    function newConnectionEvent () {
      if (subConnectEventReceived && pubConnectEventReceived) {
        e.close(function () {
          t.end()
        })
      }
    }
  })

  test('ioredis error event', function (t) {
    var e = isCluster ? builder({ cluster: { nodes: ['127'] } }) : builder({ host: '127' })

    t.plan(1)

    e.state.once('error', function (err) {
      if (isCluster) {
        t.deepEqual(err.message, 'Failed to refresh slots cache.')
      } else {
        t.deepEqual(err.message.substr(0, 7), 'connect')
      }
      e.close(function () {
        t.end()
      })
    })
  })

  test('topic pattern adapter', function (t) {
    var e = builder()

    var mqttTopic = 'rooms/+/devices/+/status'
    var expectedRedisPattern = 'rooms/*/devices/*/status'

    var subTopic = e._subTopic(mqttTopic)

    t.plan(1)

    t.deepEqual(subTopic, expectedRedisPattern)

    e.close(function () {
      t.end()
    })
  })
}
module.exports = buildTests
