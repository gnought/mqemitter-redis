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
    // if (that.pending > 0) {
    //   that.pending -= that._matcher.match(packet.msg.topic).length
    // } else {
    //   that.pending = 0
    // }
    // console.log(packet)
    // if (that.closed) return
    // if (packet.id === 'callback') {
    //   var eventname = packet.msg.payload
    //   // if (!that._cache.has(eventname)) {
    //     console.log('callback', eventname)
    //     that.state.emit(eventname)
    //   // } else {
    //   //   that._cache.set(packet.id, true, 30000)
    //   // }
    //   return
    // }
    // if (that._cb[packet.msg.topic]) {
    //   that._cb[packet.msg.topic]--
    // }
    // if (that._cb[packet.msg.topic] == 0)
    // if (!that._matcher.match(packet.msg.topic)) {
    //   console.log('not found')
    //   return
    // }
    console.log('redis msg', packet.msg)
    if (!that._cache.has(packet.id)) {
      console.log('redis msg - real emit', packet.msg)
      // that._emit(packet.msg)
      var cb = () => {
        var cb = that._cb.peekFront()
        if (cb && cb.id === packet.id) {
          var event =  that._cb.shift()
          if (event) {
            console.log(event)
            that.state.emit(event.msg.payload)
          }
        }
        return
      }
      that._emit(packet.msg, cb)
    }
    console.log('redis msg cache')
    that._cache.set(packet.id, true)
    // if (that._cb[packet.msg.topic] === undefined) {
    //   return
    // }
    // var cb = that._cb[packet.msg.topic].shift()
    // if (cb.expected > 1) {
    //   cb.expected--
    //   that._cb[packet.msg.topic].unshift(cb)
    // }
    //   if (that._cb[packet.msg.topic].expected !== 0) that._cb[packet.msg.topic].expected
    // }
    // that._cb[msg.topic] = { expected: that._matcher.match(msg.topic).length, cb: event}
  }

  this.subConn.on('messageBuffer', function (topic, message) {
    console.log('messageBuffer',  msgpack.decode(message))
    handler(topic, topic, message)
  })

  this.subConn.on('pmessageBuffer', function (sub, topic, message) {
    console.log('pmessageBuffer',  msgpack.decode(message))
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
  // this._cb = {}
  // this._cbdeferqueue = fastq(function (task, cb) { task(cb || nop) }, 1)
  // this._cbdeferqueue.pause()
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
      console.log('closed')

      that.pubConn.disconnect(false)
      that.subConn.disconnect(false)
      // that.subConn.quit().catch(nop)
      // that.pubConn.quit().catch(nop)

      that._topics = {}
      that._matcher.clear()
      that._cache.reset()
      that._queue.killAndDrain()

      // var cleanup = function () {
      //     // that._cb.clear()
      //   if (!that.closed) {
      //     that._close(cb)
      //   }
      // }

      // if (that.pubConn.status !== 'close' || that.pubConn.status !== 'end') {
      // that.pubConn.disconnect(false)
        // that.pubConn.quit(cleanup).catch(nop)
      // }
      // if (that.subConn.status !== 'close' || that.subConn.status !== 'end') {
      // that.subConn.disconnect(false)
        // that.subConn.quit(cleanup).catch(nop)
      // }
      // that._close(cb)
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
    assert('function' === typeof done)
    onFinish = function () {
      console.log('sub done')
      setImmediate(done)
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
      this._queue.push((cb) => { that.subConn.psubscribe(subTopic, cb).catch(nop) }, onFinish)
    // } else {
    //   this.subConn.psubscribe(subTopic)
    // }
  } else {
    // if (done) {
      this._queue.push((cb) => { that.subConn.subscribe(subTopic, cb).catch(nop) }, onFinish)
    // } else {
    //   this.subConn.subscribe(subTopic)
    // }
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

    console.log(that._matcher)
    console.log(msg.topic)
    console.log(that._matcher.match(msg.topic).length)
    if (that._matcher.match(msg.topic).length === 0 ) {
      console.log('emit', msg.topic)
      // // return this._queue.push((cb) => { that.pubConn.publish(msg.topic, msgpack.encode(packet), cb).catch(nop) }, onFinish)
      return onPublish(onFinish)
    }
    this._queue.push((cb) => {
      var event = 'callback_' + packet.id
      var cbPacket = {
        id: packet.id,
        msg: { payload: event }
      }
      console.log('event', event)
      this.state.once(event, () => {
        console.log('capture')
        setImmediate(done)
      })
      console.log('push', event)
      that._cb.push(cbPacket)
      cb()
    }, nop)
  }
  console.log('emit final', msg.topic)
  onPublish()
  // this._queue.push((cb) => { that.pubConn.publish(msg.topic, msgpack.encode(packet), cb).catch(nop) }, nop)



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
  var that = this

  var subTopic = this._subTopic(topic)
  var onFinish = function () {
    if (done) {
      // setImmediate(done)
      that._queue.push((cb) => { cb() }, done)
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
