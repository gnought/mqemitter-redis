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
const Denque = require("denque")

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

  this._id = hyperid().replace(/\+/g, '_').replace(/\//g, '-').replace(/==$/, '-')

  this._topics = {}

  this._cache = new LRU({
    max: 10000,
    maxAge: 60 * 1000 // one minute
  })

  this.state = new EE()

  var that = this

  function handler (sub, topic, payload) {
    var packet = msgpack.decode(payload)
    // if (that.pending > 0) {
    //   that.pending -= that._matcher.match(packet.msg.topic).length
    // } else {
    //   that.pending = 0
    // }
    // console.log(packet)
    if (that.closed) return
    if (packet.id === 'callback') {
      console.log('callback', packet.msg.payload)
      return that.state.emit(packet.msg.payload)
    }
    if (!that._matcher.match(packet.msg.topic)) {
      console.log('not found')
      return
    }
    if (!that._cache.get(packet.id)) {
      that._emit(packet.msg)
    }
    that._cache.set(packet.id, true)
    console.log('msg', packet.msg)
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
    if (err.code === 'EPIPE') {
      // safe ignore EPIPE https://github.com/luin/ioredis/issues/768
      return
    }
    that.state.emit('error', err)
    // that._queue.resume()
  })

  this.pubConn.on('connect', function () {
    that.state.emit('pubConnect')
  })

  this.pubConn.on('error', function (err) {
    that.state.emit('error', err)
    // that._queue.resume()
  })

  MQEmitter.call(this, opts)

  this._opts.regexWildcardOne = new RegExp(this._opts.wildcardOne.replace(/([/,!\\^${}[\]().*+?|<>\-&])/g, '\\$&'), 'g')

  // this.pending = 0

  this._queue = fastq(function (task, cb) { task(cb || nop) }, 1)
  this._cb = new Denque()
  // this._queue.pause()
  // this.subConn.once('ready', that._queue.resume)
}

inherits(MQEmitterRedis, MQEmitter)

;['emit', 'on', 'removeListener', 'close'].forEach(function (name) {
  MQEmitterRedis.prototype['_' + name] = MQEmitterRedis.prototype[name]
})

MQEmitterRedis.prototype.close = function (done) {
  var that = this
  console.log('closing')
  this._queue.push((cb) => {
    // that._queue.killAndDrain()
    setImmediate(() => {
      console.log('closed')
      var cleanup = function () {
        if (!that.closed) {

          Object.keys(that._topics).forEach(function (t) {
            that.removeListener(t, nop)
          })

          that._topics = {}
          that._matcher.clear()
          that._cache.prune()
          that._cb.clear()
          that._close(cb)
        }
      }
      if (that.pubConn.status !== 'close' || that.pubConn.status !== 'end') {
        that.pubConn.disconnect(false)
        that.pubConn.quit(cleanup).catch(nop)
      }
      if (that.subConn.status !== 'close' || that.subConn.status !== 'end') {
        that.subConn.disconnect(false)
        that.subConn.quit(cleanup).catch(nop)
      }
    })
  }, done || nop)

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
    assert('function' === typeof done)
    onFinish = function () {
      console.log('sub done')
      setImmediate(done)
      // done()
    }
  }
  var subTopic = this._subTopic(topic)

  this._on(topic, cb)
  // if (this._topics[subTopic]) {
  //   this.pending = this._matcher.match(topic).length
  // } else {
  //   this.pending += this._matcher.match(topic).length
  // }
  if (this._topics[subTopic]) {
    this._topics[subTopic]++
    // if (done) {
      onFinish()
      // setImmediate(onFinish)
    // }
    return this
  }

  this._topics[subTopic] = 1

  var that = this

  if (this._containsWildcard(topic)) {
    // if (done) {
      this._queue.push((cb) => { that.subConn.psubscribe(subTopic, cb) }, onFinish)
    // } else {
    //   this.subConn.psubscribe(subTopic)
    // }
  } else {
    // if (done) {
      this._queue.push((cb) => { that.subConn.subscribe(subTopic, cb) }, onFinish)
    // } else {
    //   this.subConn.subscribe(subTopic)
    // }
  }

  return this
}

MQEmitterRedis.prototype.emit = function (msg, done) {
  var that = this

  var packet = {
    id: hyperid(),
    msg: msg
  }

  if (done) {
    assert('function' === typeof done)
    if (this.closed) {
      return done(new Error('mqemitter-redis is closed'))
    }
    var onFinish = function () {
      setImmediate(done)
    }
    if (that._matcher.match(msg.topic).length === 0 ) {
      return this._queue.push((cb) => { that.pubConn.publish(msg.topic, msgpack.encode(packet), cb) }, onFinish)
    }
    var event = 'callback_' + packet.id
    // console.log(that._id)
    var cbPacket = {
      id: 'callback',
      msg: { topic: msg.topic, payload: event }
    }
    console.log('event', event)
    this.state.once(event, () => {
      console.log('capture')
      that._cb.pop(event)
      setImmediate(done)
    })
    this._cb.push(event)
    this._queue.push((cb) => {
      that.pubConn.publish(msg.topic, msgpack.encode(packet), () => {
        that.pubConn.publish(msg.topic, msgpack.encode(cbPacket), cb) })
      }
    , nop)
  } else {
    return this._queue.push((cb) => { that.pubConn.publish(msg.topic, msgpack.encode(packet), cb) }, nop)
  }


  // that.subConn.once('ready', that._queue.resume)
  // if (that.subConn.status !== 'ready') {
  //   that._queue.pause()
  // }

  // if (done) {
  //   this._queue.push((cb) => {
  //     while (that.pending > 0) {
  //       // console.log(that.pending)
  //       deasync.runLoopOnce()
  //       // deasync.sleep(1000)
  //     }
  //     setTimeout(cb)
  //   }, done)
  // }
  // this._queue.push((cb) => {
  //   var callback = (cb) => {
  //     if (cb) {
  //       if (that.pending > 0) {
  //         return setTimeout((cb) => { callback(cb) }, 200)
  //       } else {
  //         console.log(that.pending)
  //         setTimeout(cb)
  //       }
  //     }
  //   }
  //   callback(cb)
  // }, done)

  // var callback = function (cb) {
  //   if (that.pending > 0) {
  //     console.log(that.pending)
  //     return that._queue.push((cb) => { setImmediate(() => { callback(cb) }) }, cb)
  //     // cb()
  //   // } else {
  //   }
  //   console.log('done')
  //   if (cb) {
  //    setImmediate(cb)
  //   }
  //   // }
  //   // return
  //   // if (cb) {
  //   //   setImmediate(cb)
  //   // }
  // }
  // callback(done)
}

MQEmitterRedis.prototype.removeListener = function (topic, cb, done) {
  var subTopic = this._subTopic(topic)
  var onFinish = function () {
    if (done) {
      setImmediate(done)
    }
  }

  // this.pending -= this._matcher.match(topic).length

  this._removeListener(topic, cb)

  this._topics[subTopic] = this._matcher.match(topic).length

  if (this._topics[subTopic] > 0) {
    onFinish()
    return this
  }

  delete this._topics[subTopic]

  var that = this

  if (this._containsWildcard(topic)) {
    this._queue.push((cb) => { that.subConn.punsubscribe(subTopic, cb) }, onFinish)
  } else {
    this._queue.push((cb) => { that.subConn.unsubscribe(subTopic, cb) }, onFinish)
  }

  return this
}

MQEmitterRedis.prototype._containsWildcard = function (topic) {
  return (topic.indexOf(this._opts.wildcardOne) >= 0) ||
         (topic.indexOf(this._opts.wildcardSome) >= 0)
}

function nop () {}

module.exports = MQEmitterRedis
