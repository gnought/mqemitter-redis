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
// const Denque = require('denque')
// var Qlobber = require('qlobber').Qlobber

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

  // this._topics = {}
  // could buffer up to 10k publish in a batch before the subscriber receives notification
  this._willConsume = new LRU({
    max: 10000,
    maxAge: 60 * 1000 // one minute
  })

  this._cache = new LRU({
    max: 10000,
    maxAge: 60 * 1000 // one minute
  })

  this.state = new EE()

  var that = this

  function handler (sub, topic, payload) {
    if (that.closed) {
      return
    }
    var packet = msgpack.decode(payload)

    debug('redis msg', packet.msg)
    if (!that._cache.has(packet.id)) {
      debug('redis msg - real emit', packet.msg)
      // this._cb[packet.id] = { callback: done || nop, matcher: matcher.slice(0) }

      var cb = that._willConsume.get(packet.id)
      if (cb !== undefined) {
        // delete that._cb[packet.id]
        var m = cb.matcher
        debug('handle', cb.matcher)
        cb.matcher = []
        that._willConsume.del(packet.id)
        // we need to define our matcher, emulate that._emit(packet.msg, cb),
        that.current++
        that._parallel(that, m, packet.msg, cb.callback)
      }
      // var cbPacket = that._cb.peekFront()
      // if (!cbPacket || cbPacket.id !== packet.id) {
      //   // console.log('no peek')
      //   // throw new Error('packet out of sync')
      //   return
      // }
      // that._cb.shift()
      // var m = cbPacket.msg.matcher

      // that.current++
      // debug('parallel')
      // console.log(m)
      // console.log(cbPacket)
      // that._parallel(that, m, packet.msg, cbPacket.msg.callback)
      // that._parallel(that, m, packet.msg, () => {
      // cbPacket.msg.matcher = []
      // var cb = that._cb.peekFront()
      // if (cbPacket.msg.event) {
      // var cbPacket = that._cb.shift()
      // if (cbPacket) {
      // debug(cbPacket)
      // that.state.emit(cbPacket.msg.event)
      // }
      // }
      // if (cbPacket.)
      // })
      // cbPacket.msg.matcher = []
      // that._emit(packet.msg, cb)
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

  this.concurrency = 0
  this._queue = fastq(function (task, cb) { task(cb || nop) }, 1)
  // this._cb = new Denque()
  // this._cb = {}
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
  var closing = function () {
    that._close(done || nop)
    that._matcher.clear()

    that._queue.push((cb) => {
      process.nextTick(() => {
        debug('closed')

        that._cache.reset()
        // this._cb.clear()
        that._willConsume.reset()
        // Object.keys(that._topics).forEach(function (t) {
        //   that.removeListener(t, nop)
        // })
        // that._topics = {}

        that.pubConn.disconnect(false)
        that.subConn.disconnect(false)
        that.subConn.quit(cb).catch(nop)
        that.pubConn.quit(cb).catch(nop)

        that._queue.killAndDrain()
      })
    }, nop)
  }
  if (that._willConsume.length > 0) {
    // defer a bit if we haven't received any notifications from redis
    setTimeout(closing, 200)
  } else {
    closing()
  }

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
    assert(typeof done === 'function')
    // assert('function' === typeof done)
    onFinish = function () {
      debug('sub done')
      setImmediate(done)
    }
  }

  var exist = this._matcher.match(topic).length

  this._on(topic, cb)
  debug('add matcher', topic)

  if (exist > 0) {
    onFinish()
    return this
  }
  var subTopic = this._subTopic(topic)

  // if (this._topics[subTopic]) {
  //   this._topics[subTopic]++
  //   onFinish()
  //   return this
  // }

  // this._topics[subTopic] = 1

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
      assert(typeof done === 'function')
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
  var matcher = this._matcher.match(msg.topic)
  // if (done) {
  // assert(typeof done === 'function')

  // var onFinish = function () {
  //   setImmediate(done)
  // }
  debug(this._matcher)
  debug(msg.topic)
  debug(this._matcher.match(msg.topic).length)
  if (matcher.length === 0) {
    debug('emit', msg.topic)
    // return onPublish(onFinish)
    return onPublish(done)
  }
  // var event = 'callback_' + packet.id
  // var cbPacket = {
  //   id: packet.id,
  //   msg: { callback: done || nop, matcher: matcher.slice(0) }
  // }
  // this._queue.push((cb) => {
  // if (done) {
  //   debug('event', event)
  //   that.state.once(event, (err) => {
  //     debug('capture')
  //     // console.log('done')
  //     // onFinish()
  //     debug(that.state.listenerCount())
  //     debug(done)
  //     setImmediate(done)
  //     // done(err)
  //     debug('capture end')
  //     // done()
  //   })
  // }
  // debug('push', event)
  // that._cb.push(cbPacket)
  // this._cb[packet.id] = { callback: done || nop, matcher: matcher.slice(0) }
  /*
    A sequence of what we publish is what we will receive. Redis/Redis cluster does the job perfectly. However the messageBuffer & pmessageBuffer event order from ioredis sometimes
    is out-of-sync especially used ioredis for a redis cluster like AWS Elasticcache. This
    is a rare case. In this case we cannot use queue (a preferrable way) to queue up all publish callbacks.
    LRU will be a second choice. Set a maxItem and maxAge and don't let it fillup memory.
    Supposely we will get notifications from Redis/Redis cluster and clean corrosponding
    item in the cache.
  */
  this._willConsume.set(packet.id, { callback: done || nop, matcher: matcher.slice(0) })
  // cb()
  // cb(that._cb.push(cbPacket))
  // }, nop)
  // }
  debug('emit - has sub', msg.topic)
  onPublish()
}

MQEmitterRedis.prototype.removeListener = function (topic, cb, done) {
  var that = this

  var subTopic = this._subTopic(topic)
  var onFinish = function () {
    if (done) {
      assert(typeof done === 'function')
      debug('remove listener done')
      setImmediate(done)
    }
  }

  debug('remove listener')

  this._removeListener(topic, cb)

  // this._topics[subTopic] = this._matcher.match(topic).length

  if (this._matcher.match(topic).length > 0) {
    onFinish()
    return this
  }
  // if (this._topics[subTopic] > 0) {
  //   onFinish()
  //   return this
  // }

  // delete this._topics[subTopic]

  debug('remove listener - real')

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
