'use strict'

function noop () {}

function buildTests (opts) {
  var builder = opts.builder
  var isCluster = opts.isCluster
  var test = opts.test

  test('actual unsubscribe from Redis', function (t) {
    var e = builder()

    e.subConn.on('message', function (topic, message) {
      if (topic.substr(0, 5) !== '$SYS/') {
        t.fail('the message should not be emitted')
      }
    })

    e.on('hello', noop)
    e.removeListener('hello', noop)
    e.emit({ topic: 'hello' }, function () {
      e.close(function () {
        t.end()
      })
    })
  })

  test('topic is in Unicode', function (t) {
    t.plan(2)

    var e = builder()

    var notBeCalled = function (message, cb) {
      t.fail('the message should not be emitted')
      cb()
    }
    e.on('👌😎', notBeCalled)
    e.emit({ topic: '👌😎' }, noop)
    t.equal(Object.keys(e._topics).length, 1, 'should be 1')
    e.removeListener('👌😎', notBeCalled)
    t.equal(Object.keys(e._topics).length, 0, 'should be 0')
    e.close(function () {
      t.end()
    })
  })

  test('multiple mqemitter-redis can share a redis', function (t) {
    t.plan(2)

    var e1 = builder()
    var e2 = builder()
    var count = 0

    e1.on('hello', function (message, cb) {
      t.ok(message, 'message received')
      cb()
    }, () => { count++; newEvent() })
    e2.on('hello', function (message, cb) {
      t.ok(message, 'message received')
      e2.close()
      cb()
    }, () => { count++; newEvent() })

    function newEvent () {
      if (count === 2) {
        e1.emit({ topic: 'hello' }, function () {
          e1.close()
        })
      }
    }
  })

  test('unsubscribe one in multiple mqemitter-redis for one redis', function (t) {
    t.plan(1)

    var e1 = builder()
    var e2 = builder()
    var count = 0

    e1.on('hello', noop, () => { count++; newEvent() })
    e1.subConn.on('message', function (topic, message) {
      if (topic) {
        t.fail('the message should not be emitted')
      }
    })
    e2.on('hello', function (message, cb) {
      t.ok(message, 'message received')
      cb()
    }, () => { count++; newEvent() })
    function newEvent () {
      if (count === 2) {
        e1.removeListener('hello', noop, () => {
          e2.emit({ topic: 'hello' }, function () {
            e2.close()
            e1.close(function () {
              t.end()
            })
          })
        })
      }
    }
  })

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
