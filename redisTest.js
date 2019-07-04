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

    e.on('👌😎', function (message, cb) {
      t.fail('the message should not be emitted')
      cb()
    })
    e.emit({ topic: '👌😎' }, noop)
    t.equal(Object.keys(e._topics).length, 1, 'should be 1')
    e.removeListener('👌😎', noop)
    t.equal(Object.keys(e._topics).length, 0, 'should be 0')
    e.close(function () {
      t.end()
    })
  })

  test('multiple mqemitter-redis for one redis', function (t) {
    t.plan(2)

    var e1 = builder()
    var e2 = builder()

    var e1SubscribeOk = false
    var e2SubscribeOk = false

    e1.on('hello', function (message, cb) {
      t.ok(message, 'message received')
      cb()
    }, () => { e1SubscribeOk = true; newEvent() })
    e2.on('hello', function (message, cb) {
      t.ok(message, 'message received')
      cb()
    }, () => { e2SubscribeOk = true; newEvent() })

    function newEvent () {
      if (e1SubscribeOk && e2SubscribeOk) {
        e1.emit({ topic: 'hello' }, function () {
          e1.close(function () {
            t.end()
          })
          e2.close(function () {})
        })
      }
    }
  })

  test('unsubscribe one in multiple mqemitter-redis for one redis', function (t) {
    t.plan(1)

    var e1 = builder()
    var e2 = builder()

    var e1SubscribeOk = false
    var e2SubscribeOk = false

    e1.on('hello', noop, () => { e1SubscribeOk = true; newEvent() })
    e1.subConn.on('message', function (topic, message) {
      if (topic.substr(0, 5) !== '$SYS/') {
        t.fail('the message should not be emitted')
      }
    })
    e2.on('hello', function (message, cb) {
      t.ok(message, 'message received')
      cb()
    }, () => { e2SubscribeOk = true; newEvent() })
    function newEvent () {
      if (e1SubscribeOk && e2SubscribeOk) {
        e1.removeListener('hello', noop, () => {
          e2.emit({ topic: 'hello' }, function () {
            e1.close(function () {
              t.end()
            })
            e2.close(function () {})
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
