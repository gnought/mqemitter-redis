'use strict'

const Redis = require('ioredis')
const MQEmitter = require('mqemitter')
const hyperid = require('hyperid')()
const inherits = require('inherits')
const LRU = require('lru-cache')
const msgpack = require('msgpack-lite')
const EE = require('events').EventEmitter
const assert = require('assert')
const fastq = require('fastq')
const Denque = require('denque')

const DEBUG = false
function debug (...args) {
  if (DEBUG) {
    console.log.apply(this, args)
  }
}

function MQEmitterRedis (opts) {
  if (!(this instanceof MQEmitterRedis)) {
    return new MQEmitterRedis(opts)
  }

  opts = opts || {}
  this._opts = opts

  var cluster = opts.cluster
  if (cluster && !Object.is(cluster, {})) {
    let nodes = cluster.nodes || []
    let clusterOptions = cluster.options || {}
    assert(nodes.length > 0, 'No Startup Nodes in cluster-enabled redis')
    this.subConn = new Redis.Cluster(nodes, clusterOptions)
    this.pubConn = new Redis.Cluster(nodes, clusterOptions)
  } else {
    this.subConn = new Redis(opts)
    this.pubConn = new Redis(opts)
  }

  this._topics = {}

  this._cache = new LRU({
    max: 10000,
    maxAge: 60 * 1000 // one minute
  })

  this.state = new EE()

  var that = this

  function handler (sub, topic, payload) {
    var packet = msgpack.decode(payload)

    debug('redis msg', packet.msg)
    if (!that._cache.has(packet.id)) {
      debug('redis msg - real emit', packet.msg)
      var cb = () => {
        var cb = that._cb.peekFront()
        if (cb && cb.id === packet.id) {
          var event = that._cb.shift()
          if (event) {
            debug(event)
            that.state.emit(event.msg.payload)
          }
        }
      }
      that._emit(packet.msg, cb)
    }
    debug('redis msg cache')
    that._cache.set(packet.id, true)
  }

  this.subConn.on('messageBuffer', function (topic, message) {
    debug('messageBuffer', msgpack.decode(message))
    handler(topic, topic, message)
  })

  this.subConn.on('pmessageBuffer', function (sub, topic, message) {
    debug('pmessageBuffer', msgpack.decode(message))
    handler(sub, topic, message)
  })

  this.subConn.on('connect', function () {
    that.state.emit('subConnect')
  })

  this.subConn.on('error', function (err) {
    if (err.code === 'EPIPE') {
      // safe ignore EPIPE https://github.com/luin/ioredis/issues/768
      return
    }
    that.state.emit('error', err)
  })

  this.pubConn.on('connect', function () {
    that.state.emit('pubConnect')
  })

  this.pubConn.on('error', function (err) {
    that.state.emit('error', err)
  })

  MQEmitter.call(this, opts)

  this._opts.regexWildcardOne = new RegExp(this._opts.wildcardOne.replace(/([/,!\\^${}[\]().*+?|<>\-&])/g, '\\$&'), 'g')

  this._queue = fastq(function (task, cb) { task(cb || nop) }, 1)

  this._cb = new Denque()
}

inherits(MQEmitterRedis, MQEmitter)

;['emit', 'on', 'removeListener', 'close'].forEach(function (name) {
  MQEmitterRedis.prototype['_' + name] = MQEmitterRedis.prototype[name]
})

MQEmitterRedis.prototype.close = function (done) {
  var that = this
  debug('closing')

  if (that.closed) {
    return
  }
  that._close(done || nop)
  // that.subConn.on('messageBuffer', nop)
  // that.subConn.on('pmessageBuffer', nop)
  // this._queue.push((cb) => {
  // Object.keys(that._topics).forEach(function (t) {
  //   that.removeListener(t, nop)
  // })
  // }, nop)
  this._queue.push((cb) => {
    process.nextTick(() => {
      debug('closed')

      that.pubConn.disconnect(false)
      that.subConn.disconnect(false)
      // that.subConn.quit().catch(nop)
      // that.pubConn.quit().catch(nop)

      that._topics = {}
      that._matcher.clear()
      that._cache.reset()
      that._queue.killAndDrain()
    })
  }, nop)

  return this
}

MQEmitterRedis.prototype._subTopic = function (topic) {
  return topic
    .replace(this._opts.regexWildcardOne, '*')
    .replace(this._opts.wildcardSome, '*')
}

MQEmitterRedis.prototype.on = function on (topic, cb, done) {
  var onFinish = nop

  if (done) {
    // assert('function' === typeof done)
    onFinish = function () {
      debug('sub done')
      setImmediate(done)
    }
  }
  var subTopic = this._subTopic(topic)

  this._on(topic, cb)

  if (this._topics[subTopic]) {
    this._topics[subTopic]++
    onFinish()
    return this
  }

  this._topics[subTopic] = 1

  var that = this

  if (this._containsWildcard(topic)) {
    this._queue.push((cb) => { that.subConn.psubscribe(subTopic, cb).catch(nop) }, onFinish)
  } else {
    this._queue.push((cb) => { that.subConn.subscribe(subTopic, cb).catch(nop) }, onFinish)
  }

  return this
}

MQEmitterRedis.prototype.emit = function (msg, done) {
  if (this.closed) {
    if (done) {
      return done(new Error('mqemitter-redis is closed'))
    }
  }
  var that = this

  var packet = {
    id: hyperid(),
    msg: msg
  }

  var onPublish = function (done) {
    that._queue.push((cb) => { that.pubConn.publish(msg.topic, msgpack.encode(packet), cb).catch(nop) }, done || nop)
  }
  var onFinish = function () {
    setImmediate(done)
  }
  if (done) {
    // assert('function' === typeof done)

    debug(that._matcher)
    debug(msg.topic)
    debug(that._matcher.match(msg.topic).length)
    if (that._matcher.match(msg.topic).length === 0) {
      debug('emit', msg.topic)
      return onPublish(onFinish)
    }
    this._queue.push((cb) => {
      var event = 'callback_' + packet.id
      var cbPacket = {
        id: packet.id,
        msg: { payload: event }
      }
      debug('event', event)
      this.state.once(event, () => {
        debug('capture')
        setImmediate(done)
      })
      debug('push', event)
      that._cb.push(cbPacket)
      cb()
    }, nop)
  }
  debug('emit final', msg.topic)
  onPublish()
}

MQEmitterRedis.prototype.removeListener = function (topic, cb, done) {
  var that = this

  var subTopic = this._subTopic(topic)
  var onFinish = function () {
    if (done) {
      debug('remove listener done')
      setImmediate(done)
    }
  }

  debug('remove listener')

  this._removeListener(topic, cb)

  this._topics[subTopic] = this._matcher.match(topic).length

  if (this._topics[subTopic] > 0) {
    onFinish()
    return this
  }

  delete this._topics[subTopic]

  if (this._containsWildcard(topic)) {
    this._queue.push((cb) => { that.subConn.punsubscribe(subTopic, cb).catch(nop) }, onFinish)
  } else {
    this._queue.push((cb) => { that.subConn.unsubscribe(subTopic, cb).catch(nop) }, onFinish)
  }

  return this
}

MQEmitterRedis.prototype._containsWildcard = function (topic) {
  return (topic.indexOf(this._opts.wildcardOne) >= 0) ||
         (topic.indexOf(this._opts.wildcardSome) >= 0)
}

function nop () {}

module.exports = MQEmitterRedis
