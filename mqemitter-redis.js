'use strict'

var Redis = require('ioredis')
var MQEmitter = require('mqemitter')
var hyperid = require('hyperid')()
var inherits = require('inherits')
var LRU = require('lru-cache')
var msgpack = require('msgpack-lite')
var EE = require('events').EventEmitter
var assert = require('assert')

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

  this._id = hyperid()
  this._topics = {}

  this._cache = new LRU({
    max: 10000,
    maxAge: 60 * 1000 // one minute
  })

  this.state = new EE()

  var that = this

  function handler (sub, topic, payload) {
    var packet = msgpack.decode(payload)
    if (!that._cache.get(packet.id)) {
      that._emit(packet.msg)
    }
    that._cache.set(packet.id, true)
  }

  this.subConn.on('messageBuffer', function (topic, message) {
    handler(topic, topic, message)
  })

  this.subConn.on('pmessageBuffer', function (sub, topic, message) {
    handler(sub, topic, message)
  })

  this.subConn.on('connect', function () {
    that.state.emit('subConnect')
  })

  this.subConn.on('error', function (err) {
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
}

inherits(MQEmitterRedis, MQEmitter)

;['emit', 'on', 'removeListener', 'close'].forEach(function (name) {
  MQEmitterRedis.prototype['_' + name] = MQEmitterRedis.prototype[name]
})

MQEmitterRedis.prototype.close = function (done) {
  var that = this

  var subConnEnd = false
  var pubConnEnd = false

  var cleanup = function () {
    if (subConnEnd && pubConnEnd) {
      that._topics = {}
      that._matcher.clear()
      that._cache.prune()
      that._close(done || nop)
    }
  }

  var handleClose = function () {
    that.subConn.quit(() => { that.subConn.disconnect(); subConnEnd = true; cleanup() })
    that.pubConn.quit(() => { that.pubConn.disconnect(); pubConnEnd = true; cleanup() })
  }

  if (that.subConn.status === 'ready') {
    var sep = that._opts.separator
    var topic = '$SYS' + sep + that._subTopic(that._id).replace(sep, '') + sep + 'redis' + sep + 'close'
    this.on(topic, () => {
      handleClose()
    }, () => {
      this.emit({ topic: topic, payload: 1 })
    })
  } else {
    handleClose()
  }

  return this
}

MQEmitterRedis.prototype._subTopic = function (topic) {
  return topic
    .replace(this._opts.regexWildcardOne, '*')
    .replace(this._opts.wildcardSome, '*')
}

MQEmitterRedis.prototype.on = function on (topic, cb, done) {
  var subTopic = this._subTopic(topic)
  var onFinish = function () {
    if (done) {
      setImmediate(done)
    }
  }

  this._on(topic, cb)

  if (this._topics[subTopic]) {
    this._topics[subTopic]++
    onFinish()
    return this
  }

  this._topics[subTopic] = 1

  if (this._containsWildcard(topic)) {
    this.subConn.psubscribe(subTopic, onFinish).catch(() => {})
  } else {
    this.subConn.subscribe(subTopic, onFinish).catch(() => {})
  }

  return this
}

MQEmitterRedis.prototype.emit = function (msg, done) {
  if (this.closed) {
    if (done) {
      return done(new Error('mqemitter-redis is closed'))
    }
  }

  var onFinish = function () {
    if (done) {
      setImmediate(done)
    }
  }
  var packet = {
    id: hyperid(),
    msg: msg
  }
  this.pubConn.ping(() => {
    this.pubConn.publish(msg.topic, msgpack.encode(packet), onFinish).catch(() => {})
  })
}

MQEmitterRedis.prototype.removeListener = function (topic, cb, done) {
  var subTopic = this._subTopic(topic)
  var onFinish = function () {
    if (done) {
      setImmediate(done)
    }
  }

  this._removeListener(topic, cb)

  if (--this._topics[subTopic] > 0) {
    onFinish()
    return this
  }

  delete this._topics[subTopic]

  if (this._containsWildcard(topic)) {
    this.subConn.punsubscribe(subTopic, onFinish)
  } else if (this._matcher.match(topic)) {
    this.subConn.unsubscribe(subTopic, onFinish)
  }

  return this
}

MQEmitterRedis.prototype._containsWildcard = function (topic) {
  return (topic.indexOf(this._opts.wildcardOne) >= 0) ||
         (topic.indexOf(this._opts.wildcardSome) >= 0)
}

function nop () {}

module.exports = MQEmitterRedis
