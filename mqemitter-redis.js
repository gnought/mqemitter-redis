'use strict'

var Redis = require('ioredis')
var MQEmitter = require('mqemitter')
var hyperid = require('hyperid')()
var inherits = require('inherits')
var LRU = require('lru-cache')
var msgpack = require('msgpack-lite')
var EE = require('events').EventEmitter
var assert = require('assert')

const sleep = (milliseconds) => {
  return new Promise(resolve => setTimeout(() => resolve('I did something'), milliseconds))
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
    this.pubConn = this.subConn.duplicate()
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
    that.pending -= 1
    if (!that._cache.get(packet.id)) {
      // console.log(packet.msg, 'emit')
      that._emit(packet.msg)
    }
    that._cache.set(packet.id, true)
  }

  this.subConn.on('messageBuffer', function (topic, message) {
    // console.log('received messageBuffer', topic)
    handler(topic, topic, message)
  })

  this.subConn.on('pmessageBuffer', function (sub, topic, message) {
    // console.log('received pmessageBuffer', topic)
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
  this._opts.regexSep = new RegExp(this._opts.separator, 'g')
  this.pending = 0
  this.concurrency = 0
}

inherits(MQEmitterRedis, MQEmitter)

;['emit', 'on', 'removeListener', 'close'].forEach(function (name) {
  MQEmitterRedis.prototype['_' + name] = MQEmitterRedis.prototype[name]
})

MQEmitterRedis.prototype.close = function (done) {
  var that = this

  // var subConnEnd = false
  // var pubConnEnd = false
  // console.log(this._id, 'close')
  var cleanup = function () {
    // if (subConnEnd && pubConnEnd) {
    console.log('cleanup')
    that._topics = {}
    that._matcher.clear()
    that._cache.prune()
    that._close(done || nop)
    // }
  }

  var handleClose = function () {
    // that.pubConn.removeListener(topic, handleClose)
    that.removeListener(topic, handleClose)
    while (that.pending > 0) {
      sleep(1000)
    }
    // console.log('handleClose')
    that.pubConn.disconnect(false)
    that.subConn.disconnect(false)
    // that.pubConn.quit(() => { pubConnEnd = true; })
    // that.subConn.quit(() => { subConnEnd = true; })
    // that.pubConn.quit()
    // that.subConn.quit()
    that.pubConn.quit().catch(nop)
    that.subConn.quit(cleanup).catch(nop)
  }
  // console.log(that.pending)
  // if ( {
  //   handleClose()
  // }
  // else {
  //   setImmediate(() => {
  //     setTimeout(handleClose, 1000)
  //   })
  // }

  var sep = that._opts.separator
  var topic = '$SYS' + sep + that._subTopic(that._id).replace(that._opts.regexSep, '') + sep + 'redis' + sep + 'close'
  // that.pubConn.ping(() => { process.nextTick(() => { pubConnEnd = true; that.pubConn.disconnect(false); that.pubConn.quit(cleanup) }) })
  // that.subConn.ping((err) => { console.log(err) })
  // console.log(that.pending)
  if (that.pending > 0 && that.subConn.status === 'ready') {
    // that.subConn.ping(() => {
    // that.pubConn.disconnect(false); that.pubConn.quit()
    // that.subConn.disconnect(false); that.subConn.quit(cleanup)
    that.on(topic, handleClose, () => {
      process.nextTick(() => {
        that.emit({ topic: topic, payload: 1 })
      })
    })
    // })
  } else {
    console.log('handleClose_offline')
    handleClose()
    // that.pubConn.disconnect(true)
    // that.subConn.disconnect(true)
    // that.pubConn.quit()
    // that.subConn.quit(cleanup)
    // process.nextTick(cleanup)
  }
  // if (that.pending > 0 && that.pubConn.status === 'ready') {
  //   var sep = that._opts.separator
  //   var topic = '$SYS' + sep + that._subTopic(that._id).replace(sep, '') + sep + 'redis' + sep + 'close'
  //   that.on(topic, () => {
  //     that.removeListener(topic, handleClose)
  //     handleClose()
  //   }, () => {
  //     this.emit({ topic: topic, payload: 1 })
  //   })
  // } else {
  //   handleClose()
  // }

  return this
}

MQEmitterRedis.prototype._subTopic = function (topic) {
  return topic
    .replace(this._opts.regexWildcardOne, '*')
    .replace(this._opts.wildcardSome, '*')
}

MQEmitterRedis.prototype.on = function on (topic, cb, done) {
  console.log('on', topic)
  var subTopic = this._subTopic(topic)
  var onFinish = function () {
    if (done) {
      setImmediate(done)
      // process.nextTick(done)
    }
  }

  this._on(topic, cb)

  this.pending += 1

  if (this._topics[subTopic]) {
    this._topics[subTopic]++
    onFinish()
    return this
  }

  this._topics[subTopic] = 1

  if (this._containsWildcard(topic)) {
    this.subConn.psubscribe(subTopic, onFinish)
  } else {
    this.subConn.subscribe(subTopic, onFinish)
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
  var onFinish = function () {
    if (done) {
      var sep = that._opts.separator
      var topic = '$SYS' + sep + that._subTopic(packet.id).replace(that._opts.regexSep, '') + sep + 'redis' + sep + 'emitCallback'
      var handleDone = function () {
        that.removeListener(topic, handleDone)
        setTimeout(done)
        // done()
      }
      that.on(topic, handleDone, () => {
        that.emit({ topic: topic, payload: 1 })
      })
      // setImmediate(done)
      // setTimeout(() => { setImmediate(done) })
    }
  }
  // var onFinish = function () {
  //   if (done) {
  //     // setImmediate(done)
  //     setTimeout(done)
  //   }
  // }

  that.pubConn.publish(msg.topic, msgpack.encode(packet), onFinish)
}

MQEmitterRedis.prototype.removeListener = function (topic, cb, done) {
  var subTopic = this._subTopic(topic)
  var onFinish = function () {
    if (done) {
      setImmediate(done)
    }
  }

  this._removeListener(topic, cb)

  this.pending -= 1

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
