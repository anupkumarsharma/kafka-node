'use strict';

const Client = require('./client');
const logger = require('./logging')('kafka-node:KafkaClient');
const EventEmitter = require('events');
const async = require('async');
const retry = require('retry');
const assert = require('assert');
const _ = require('lodash');
const util = require('util');
const net = require('net');
const BufferList = require('bl');
const tls = require('tls');
const BrokerWrapper = require('./wrapper/BrokerWrapper');
const errors = require('./errors');
const validateConfig = require('./utils').validateConfig;
const TimeoutError = require('./errors/TimeoutError');
const NotControllerError = require('./errors/NotControllerError');
const protocol = require('./protocol');
const protocolVersions = require('./protocol/protocolVersions');
const baseProtocolVersions = protocolVersions.baseSupport;
const apiMap = protocolVersions.apiMap;
const NestedError = require('nested-error-stacks');
const getCodec = require('./codec');

const DEFAULTS = {
  kafkaHost: 'localhost:9092',
  connectTimeout: 10000,
  requestTimeout: 30000,
  idleConnection: 5 * 60 * 1000,
  autoConnect: true,
  versions: {
    disabled: false,
    requestTimeout: 500
  },
  connectRetryOptions: {
    retries: 5,
    factor: 2,
    minTimeout: 1 * 1000,
    maxTimeout: 60 * 1000,
    randomize: true
  },
  maxAsyncRequests: 10,
  noAckBatchOptions: null
};

const KafkaClient = function (options) {
  EventEmitter.call(this); // Intentionally not calling Client to avoid constructor logic
  this.options = _.defaultsDeep(options || {}, DEFAULTS);

  this.sslOptions = this.options.sslOptions;
  this.ssl = !!this.sslOptions;

  if (this.options.ssl === true) {
    this.options.ssl = {};
  }

  if (this.options.clientId) {
    validateConfig('clientId', this.options.clientId);
  }

  this.clientId = this.options.clientId || 'kafka-node-client';
  this.noAckBatchOptions = this.options.noAckBatchOptions;
  this.brokers = {};
  this.longpollingBrokers = {};
  this.topicMetadata = {};
  this.correlationId = 0;
  this._socketId = 0;
  /**
   * @type {Map<any, Map<any, any>>}
   */
  this.cbqueue = new Map();
  this.brokerMetadata = {};
  this.clusterMetadata = {};
  this.ready = false;

  this.initialHosts = parseHostList(this.options.kafkaHost);

  if (this.options.autoConnect) {
    this.connect();
  }
};

util.inherits(KafkaClient, Client);

/*
{ '1001':
   { jmx_port: -1,
     timestamp: '1492521177416',
     endpoints: [ 'PLAINTEXT://127.0.0.1:9092', 'SSL://127.0.0.1:9093' ],
     host: '127.0.0.1',
     version: 2,
     port: '9092',
     id: '1001' } }

     vs

{ '1001': { nodeId: 1001, host: '127.0.0.1', port: 9093 } }

     */

function parseHost (hostString) {
  const ip = hostString.substring(0, hostString.lastIndexOf(':'));
  const port = hostString.substring(hostString.lastIndexOf(':') + 1);
  const isIpv6 = ip.match(/\[(.*)\]/);
  const host = isIpv6 ? isIpv6[1] : ip;
  return {
    host,
    port
  };
}

function parseHostList (hosts) {
  return hosts.split(',').map(parseHost);
}

KafkaClient.prototype.connect = function () {
  if (this.connecting) {
    logger.debug('connect request ignored. Client is currently connecting');
    return;
  }
  this.connecting = true;

  const connect = retry.operation(this.options.connectRetryOptions);
  if (this.currentConnect) {
    this.currentConnect.stop();
    this.currentConnect = null;
  }
  this.currentConnect = connect;

  connect.attempt(currentAttempt => {
    if (this.closing) {
      logger.debug('Client is closing abort retry');
      connect.stop();
      return;
    }

    logger.debug(`Connect attempt ${currentAttempt}`);

    async.series(
      [
        callback => {
          this.connectToBrokers(this.initialHosts, callback);
        },

        callback => {
          if (this.closing) {
            logger.debug('Client is closing abort retry');
            connect.stop();
            return;
          }
          this.loadMetadataForTopics([], (error, result) => {
            if (error) {
              logger.debug('loadMetadataForTopics after connect failed', error);
              return callback(error);
            }
            this.updateMetadatas(result, true);
            callback(null);
          });
        }
      ],
      error => {
        if (connect.retry(error)) {
          return;
        }

        this.connecting = false;

        if (error) {
          logger.debug('exhausted retries. Main error', connect.mainError());
          this.emit('error', connect.mainError());
          return;
        }

        this.ready = true;
        this.emit('ready');
      }
    );
  });
};

KafkaClient.prototype.connectToBrokers = function (hosts, callback) {
  assert(hosts && hosts.length, 'No hosts to connect to');
  hosts = _.shuffle(hosts);
  let index = 0;
  let errors = [];
  let broker = null;
  async.doWhilst(
    callback => {
      this.connectToBroker(hosts[index++], (error, connectedBroker) => {
        if (error) {
          logger.debug('failed to connect because of ', error);
          errors.push(error);
          callback(null);
          return;
        }
        errors.length = 0;
        broker = connectedBroker;
        callback(null);
      });
    },
    () => !this.closing && !broker && index < hosts.length,
    () => {
      if (broker) {
        return callback(null, broker);
      }

      logger.debug('Could not connect to any brokers');
      if (errors.length) {
        callback(errors.pop());
      } else {
        callback(new Error('client is closing?'));
      }
    }
  );
};

KafkaClient.prototype.connectToBroker = function (broker, callback) {
  const timeout = this.options.connectTimeout;
  logger.debug(`Trying to connect to host: ${broker.host} port: ${broker.port}`);
  let connectTimer = null;

  callback = _.once(callback);

  const brokerWrapper = this.setupBroker(broker.host, broker.port, false, this.brokers);
  const socket = brokerWrapper.socket;
  function onError (error) {
    logger.debug('Socket Error', error);
    clearTimeout(connectTimer);
    callback(error);
  }
  socket.once('error', onError);

  socket.once('connect', () => {
    logger.debug('broker socket connected %j', broker);
    clearTimeout(connectTimer);
    socket.removeListener('error', onError);
    callback(null, brokerWrapper);
  });
  connectTimer = setTimeout(function () {
    logger.debug('Connection timeout error with broker %j', broker);
    onError(new TimeoutError(`Connection timeout of ${timeout}ms exceeded`));
  }, timeout);
};

KafkaClient.prototype.getController = function (callback) {
  // Check for cached controller
  if (this.clusterMetadata.controllerId) {
    var controller = this.brokerMetadata[this.clusterMetadata.controllerId];
    var broker = this.getBroker(controller.host, controller.port);

    return callback(null, broker);
  }

  // If cached controller is not available, refresh metadata
  this.loadMetadata((error, result) => {
    if (error) {
      return callback(error);
    }

    // No controller will be available if api version request timed out, or if kafka version is less than 0.10.
    if (!result[1].clusterMetadata || result[1].clusterMetadata.controllerId == null) {
      return callback(new errors.BrokerNotAvailableError('Controller broker not available'));
    }

    this.updateMetadatas(result);

    var controllerId = result[1].clusterMetadata.controllerId;
    var controllerMetadata = result[0][controllerId];

    var broker = this.getBroker(controllerMetadata.host, controllerMetadata.port);

    if (!broker || !broker.isConnected()) {
      return callback(new errors.BrokerNotAvailableError('Controller broker not available'));
    }

    return callback(null, broker);
  });
};

KafkaClient.prototype.getBroker = function (host, port, longpolling) {
  const brokers = this.getBrokers();

  var addr = host + ':' + port;
  return brokers[addr] || this.setupBroker(host, port, longpolling, brokers);
};

KafkaClient.prototype.setupBroker = function (host, port, longpolling, brokers) {
  var brokerKey = host + ':' + port;
  brokers[brokerKey] = this.createBroker(host, port, longpolling);
  return brokers[brokerKey];
};

// returns a connected broker
KafkaClient.prototype.getAvailableBroker = function (callback) {
  const brokers = this.getBrokers();
  const connectedBrokers = _.filter(brokers, function (broker) {
    return broker.isConnected();
  });

  if (connectedBrokers.length) {
    logger.debug('found %d connected broker(s)', connectedBrokers.length);
    return callback(null, _.sample(connectedBrokers));
  }

  let brokersToTry;

  if (_.isEmpty(brokers)) {
    brokersToTry = _.values(this.brokerMetadata);
  } else {
    const badBrokers = Object.keys(brokers);
    brokersToTry = _.filter(this.brokerMetadata, function (broker) {
      return !_.includes(badBrokers, `${broker.host}:${broker.port}`);
    });
  }

  if (_.isEmpty(brokersToTry)) {
    return callback(new Error('Unable to find available brokers to try'));
  }

  this.connectToBrokers(brokersToTry, callback);
};

KafkaClient.prototype.refreshBrokers = function () {
  var self = this;
  var validBrokers = _.map(this.brokerMetadata, function (broker) {
    return `${broker.host}:${broker.port}`;
  });

  function closeDeadBrokers (brokers) {
    var deadBrokerKeys = _.difference(Object.keys(brokers), validBrokers);
    if (deadBrokerKeys.length) {
      self.closeBrokers(
        deadBrokerKeys.map(function (key) {
          var broker = brokers[key];
          delete brokers[key];
          return broker;
        })
      );
    }
  }

  closeDeadBrokers(this.brokers);
  closeDeadBrokers(this.longpollingBrokers);
};

KafkaClient.prototype.refreshBrokerMetadata = function (callback) {
  if (this.refreshingMetadata || this.closing) {
    return;
  }

  if (callback == null) {
    callback = _.noop;
  }

  this.refreshingMetadata = true;

  async.waterfall(
    [callback => this.getAvailableBroker(callback), (broker, callback) => this.loadMetadataFrom(broker, callback)],
    (error, result) => {
      this.refreshingMetadata = false;
      if (error) {
        callback(error);
        return this.emit('error', new NestedError('refreshBrokerMetadata failed', error));
      }
      this.updateMetadatas(result, true);
      this.refreshBrokers();
      callback(error);
    }
  );
};

KafkaClient.prototype.loadMetadataFrom = function (broker, cb) {
  assert(broker && broker.isConnected());
  var correlationId = this.nextId();
  var request = protocol.encodeMetadataRequest(this.clientId, correlationId, []);

  this.queueCallback(broker.socket, correlationId, [protocol.decodeMetadataResponse, cb]);
  broker.write(request);
};

KafkaClient.prototype.setBrokerMetadata = function (brokerMetadata) {
  assert(brokerMetadata, 'brokerMetadata is empty');
  const oldBrokerMetadata = this.brokerMetadata;
  this.brokerMetadata = brokerMetadata;
  this.brokerMetadataLastUpdate = Date.now();

  if (!_.isEmpty(oldBrokerMetadata) && !_.isEqual(oldBrokerMetadata, brokerMetadata)) {
    setImmediate(() => this.emit('brokersChanged'));
  }
};

KafkaClient.prototype.setClusterMetadata = function (clusterMetadata) {
  assert(clusterMetadata, 'clusterMetadata is empty');
  this.clusterMetadata = clusterMetadata;
};

KafkaClient.prototype.setControllerId = function (controllerId) {
  if (!this.clusterMetadata) {
    this.clusterMetadata = {
      controllerId
    };

    return;
  }
  this.clusterMetadata.controllerId = controllerId;
};

KafkaClient.prototype.updateMetadatas = function (metadatas, replaceTopicMetadata) {
  assert(metadatas && Array.isArray(metadatas) && metadatas.length === 2, 'metadata format is incorrect');
  logger.debug('updating metadatas');
  this.setBrokerMetadata(metadatas[0]);
  if (replaceTopicMetadata) {
    this.topicMetadata = metadatas[1].metadata;
  } else {
    _.extend(this.topicMetadata, metadatas[1].metadata);
  }

  if (metadatas[1].clusterMetadata) {
    this.setClusterMetadata(metadatas[1].clusterMetadata);
  }
};

KafkaClient.prototype.brokerForLeader = function (leader, longpolling) {
  var addr;
  var brokers = this.getBrokers(longpolling);
  // If leader is not give, choose the first broker as leader
  if (typeof leader === 'undefined') {
    if (!_.isEmpty(brokers)) {
      addr = Object.keys(brokers)[0];
      return brokers[addr];
    } else if (!_.isEmpty(this.brokerMetadata)) {
      leader = Object.keys(this.brokerMetadata)[0];
    } else {
      return;
    }
  }

  var broker = this.brokerMetadata[leader];

  if (!broker) {
    return;
  }

  addr = broker.host + ':' + broker.port;

  return brokers[addr] || this.setupBroker(broker.host, broker.port, longpolling, brokers);
};

KafkaClient.prototype.wrapTimeoutIfNeeded = function (socketId, correlationId, callback, overrideTimeout) {
  const timeout = overrideTimeout || this.options.requestTimeout;
  if (!timeout) {
    return callback;
  }

  let timeoutId = null;

  const wrappedFn = function () {
    clear();
    callback.apply(null, arguments);
  };

  function clear () {
    clearTimeout(timeoutId);
    timeoutId = null;
  }

  timeoutId = setTimeout(() => {
    this.unqueueCallback(socketId, correlationId);
    callback(new TimeoutError(`Request timed out after ${timeout}ms`));
    callback = _.noop;
  }, timeout).unref();

  wrappedFn.timeoutId = timeoutId;

  return wrappedFn;
};

KafkaClient.prototype.queueCallback = function (socket, id, data) {
  data[1] = this.wrapTimeoutIfNeeded(socket.socketId, id, data[1], data[2]);
  Client.prototype.queueCallback.call(this, socket, id, data);
};

KafkaClient.prototype.getApiVersions = function (broker, cb) {
  if (!broker || !broker.isConnected()) {
    return cb(new errors.BrokerNotAvailableError('Broker not available (getApiVersions)'));
  }

  logger.debug(`Sending versions request to ${broker.socket.addr}`);

  const correlationId = this.nextId();
  const request = protocol.encodeVersionsRequest(this.clientId, correlationId);

  this.queueCallback(broker.socket, correlationId, [
    protocol.decodeVersionsResponse,
    cb,
    this.options.versions.requestTimeout
  ]);
  broker.write(request);
};

KafkaClient.prototype.getListGroups = function (callback) {
  if (!this.ready) {
    return callback(new Error('Client is not ready (getListGroups)'));
  }
  const brokers = this.brokerMetadata;
  async.mapValuesLimit(
    brokers,
    this.options.maxAsyncRequests,
    (brokerMetadata, brokerId, cb) => {
      const broker = this.brokerForLeader(brokerId);
      if (!broker || !broker.isConnected()) {
        return cb(new errors.BrokerNotAvailableError('Broker not available (getListGroups)'));
      }

      const correlationId = this.nextId();
      const request = protocol.encodeListGroups(this.clientId, correlationId);
      this.queueCallback(broker.socket, correlationId, [protocol.decodeListGroups, cb]);
      broker.write(request);
    },
    (err, results) => {
      if (err) {
        callback(err);
        return;
      }
      results = _.values(results);
      callback(null, _.merge.apply({}, results));
    }
  );
};

KafkaClient.prototype.getDescribeGroups = function (groups, callback) {
  if (!this.ready) {
    return callback(new Error('Client is not ready (getDescribeGroups)'));
  }

  async.groupByLimit(
    groups,
    this.options.maxAsyncRequests,
    (group, cb) => {
      this.sendGroupCoordinatorRequest(group, (err, coordinator) => {
        cb(err || null, coordinator ? coordinator.coordinatorId : undefined);
      });
    },
    (err, results) => {
      if (err) {
        callback(err);
        return;
      }

      async.mapValuesLimit(
        results,
        this.options.maxAsyncRequests,
        (groups, coordinator, cb) => {
          const broker = this.brokerForLeader(coordinator);
          if (!broker || !broker.isConnected()) {
            return cb(new errors.BrokerNotAvailableError('Broker not available (getDescribeGroups)'));
          }

          const correlationId = this.nextId();
          const request = protocol.encodeDescribeGroups(this.clientId, correlationId, groups);
          this.queueCallback(broker.socket, correlationId, [protocol.decodeDescribeGroups, cb]);
          broker.write(request);
        },
        (err, res) => {
          if (err) {
            return callback(err);
          }

          callback(
            null,
            _.reduce(
              res,
              (result, describes, broker) => {
                _.each(describes, (values, consumer) => {
                  result[consumer] = values;
                  result[consumer].brokerId = broker;
                });
                return result;
              },
              {}
            )
          );
        }
      );
    }
  );
};

KafkaClient.prototype.close = function (callback) {
  if (this.currentConnect) {
    this.currentConnect.stop();
    this.currentConnect = null;
  }
  const self = this;
  if (!this.pendingClose) {
    this.pendingClose = new Promise((resolve, reject) => {
      self.pendingCloseResolve = resolve;
    });
    // Make any dead connections die faster
    Object.keys(this.brokers).forEach(broker => {
      const socket = this.brokers[broker].socket;
      if (socket) {
        socket.setTimeout(5000);
      }
    });
  }
  if (callback) {
    this.pendingClose.then(() => {
      callback();
    });
  }
  logger.debug('close client');
  this.closing = true;
  if (this.cbqueue.size || this.closed) {
    // Wait for the last CB to finish
    return;
  }
  this.closed = true;
  this.closeBrokers(this.brokers);
  this.closeBrokers(this.longpollingBrokers);
  this.pendingCloseResolve();
};

KafkaClient.prototype.initializeBroker = function (broker, callback) {
  if (!broker || !broker.isConnected()) {
    return callback(new errors.BrokerNotAvailableError('Broker not available (initializeBroker)'));
  }

  if (this.options.versions.disabled) {
    callback(null);
    return;
  }

  this.getApiVersions(broker, (error, versions) => {
    if (error) {
      if (error instanceof TimeoutError) {
        logger.debug('getApiVersions request timedout probably less than 0.10 using base support');
        versions = baseProtocolVersions;
      } else {
        logger.error('ApiVersions failed with unexpected error', error);
        callback(error);
        return;
      }
    } else {
      logger.debug(`Received versions response from ${broker.socket.addr}`);
    }

    if (_.isEmpty(versions)) {
      return callback(new Error(`getApiVersions response was empty for broker: ${broker}`));
    }

    logger.debug('setting api support to %j', versions);
    broker.apiSupport = versions;
    callback(null);
  });
};

KafkaClient.prototype.createBroker = function (host, port, longpolling) {
  var self = this;
  if (this.closing) {
    throw new Error('Client is closing');
  }
  var socket;
  if (self.ssl) {
    socket = tls.connect(port, host, self.sslOptions);
  } else {
    socket = net.createConnection(port, host);
  }
  const brokerKey = `${host}:${port}`;
  socket.addr = host + ':' + port;
  socket.host = host;
  socket.port = port;
  socket.socketId = this.nextSocketId();
  if (longpolling) socket.longpolling = true;

  socket.on('timeout', function () {
    socket.error = new TimeoutError(`Connection timeout`);
    socket.end();
  });
  socket.on('connect', function () {
    const lastError = this.error;
    this.error = null;
    if (lastError) {
      this.waiting = false;

      self.initializeBroker(brokerWrapper, function (error) {
        if (error) {
          logger.error('error initialize broker after reconnect', error);
        } else {
          const readyEventName = brokerWrapper.getReadyEventName();
          self.emit(readyEventName);
        }
        self.emit('reconnect');
      });
    } else {
      self.initializeBroker(brokerWrapper, function (error) {
        if (error) {
          logger.error('error initialize broker after connect', error);
        } else {
          const readyEventName = brokerWrapper.getReadyEventName();
          self.emit(readyEventName);
        }
        self.emit('connect');
      });
    }
  });
  socket.on('error', function (err) {
    socket.error = err;
    if (!self.connecting) {
      self.emit('socket_error', err);
    }
  });
  socket.on('close', function () {
    self.emit('close');
    logger.debug(`Socket Closed ${brokerKey}`);
    if (!self.closing || socket.error) {
      self.clearCallbackQueue(socket,
        socket.error || new errors.BrokerNotAvailableError('Broker not available')
      );
    } else {
      self.clearCallbackQueue(socket);
    }
    socket.closing = true;
    socket.end();
    socket.destroy();
    socket.unref();
    delete self.brokers[brokerKey];
    if (!self.closing) {
      retry();
    }
  });
  socket.on('end', function () {
    retry();
  });
  socket.buffer = new BufferList();
  socket.on('data', function (data) {
    socket.buffer.append(data);
    self.handleReceivedData(socket);
  });
  socket.setKeepAlive(true, 60000);

  const brokerWrapper = new BrokerWrapper(socket, this.noAckBatchOptions, this.options.idleConnection);

  function retry () {
    if (socket.retrying || socket.closing || self.closing) return;
    socket.retrying = true;
    socket.retryTimer = setTimeout(function () {
      if (socket.closing) return;
      if (brokerWrapper.isIdle()) {
        logger.debug(`${self.clientId} to ${socket.addr} is idle not reconnecting`);
        socket.closing = true;
        self.deleteDisconnected(brokerWrapper);
        return;
      }
      logger.debug(`${self.clientId} reconnecting to ${socket.addr}`);
      self.reconnectBroker(socket);
    }, 1000).unref();
  }
  return brokerWrapper;
};

KafkaClient.prototype.deleteDisconnected = function (broker) {
  if (!broker.isConnected()) {
    const brokers = this.getBrokers(broker.socket.longpolling);
    const key = broker.socket.addr;
    assert(brokers[key] === broker);
    delete brokers[key];
  }
};

KafkaClient.prototype.clearCallbackQueue = function (socket, error) {
  const socketId = socket.socketId;
  const longpolling = socket.longpolling;

  const queue = this.cbqueue.get(socketId);
  if (!queue) {
    return;
  }

  if (!longpolling) {
    queue.forEach(function (handlers) {
      const cb = handlers[1];
      if (error) {
        cb(error);
      }

      if (cb.timeoutId != null) {
        clearTimeout(cb.timeoutId);
      }
    });
  }
  this.cbqueue.delete(socketId);
  if (this.closing) {
    this.close();
  }
};

/**
 * Fetches metadata for brokers and cluster.
 * This includes an array containing each node (id, host and port).
 * Depending on kafka version, additional cluster information is available (controller id).
 * @param {loadMetadataCallback} cb Function to call once metadata is loaded.
 */
KafkaClient.prototype.loadMetadata = function (callback) {
  this.loadMetadataForTopics(null, callback);
};

/**
 * Fetches metadata for brokers and cluster.
 * This includes an array containing each node (id, host and port). As well as an object
 * containing the topic name, partition, leader number, replica count, and in sync replicas per partition.
 * Depending on kafka version, additional cluster information is available (controller id).
 * @param {Array} topics List of topics to fetch metadata for. An empty array ([]) will fetch all topics.
 * @param {loadMetadataCallback} callback Function to call once metadata is loaded.
 */
KafkaClient.prototype.loadMetadataForTopics = function (topics, callback) {
  var broker = this.brokerForLeader();

  if (!broker || !broker.socket || broker.socket.error || broker.socket.destroyed) {
    return callback(new errors.BrokerNotAvailableError('Broker not available'));
  }

  const ensureBrokerReady = (broker, cb) => {
    if (!broker.isReady()) {
      logger.debug('missing apiSupport waiting until broker is ready...');
      this.waitUntilReady(broker, cb);
    } else {
      cb(null);
    }
  };

  async.series([
    cb => {
      ensureBrokerReady(broker, cb);
    },
    cb => {
      var correlationId = this.nextId();
      var supportedCoders = getSupportedForRequestType(broker, 'metadata');
      var request = supportedCoders.encoder(this.clientId, correlationId, topics);

      this.queueCallback(broker.socket, correlationId, [supportedCoders.decoder, cb]);
      broker.write(request);
    }
  ], (err, result) => {
    callback(err, result[1]);
  });
};

/**
 * Creates one or more topics.
 * @param {Array} topics Array of topics with partition and replication factor to create.
 * @param {createTopicsCallback} callback Function to call once operation is completed.
 */
KafkaClient.prototype.createTopics = function (topics, callback) {
  // Calls with [string, string, ...] are forwarded to support previous versions
  if (topics.every(t => typeof t === 'string')) {
    return Client.prototype.createTopics.apply(this, arguments);
  }

  const encoder = protocol.encodeCreateTopicRequest;
  const decoder = protocol.decodeCreateTopicResponse;

  this.sendControllerRequest(encoder, decoder, [topics, this.options.requestTimeout], callback);
};

KafkaClient.prototype.topicExists = function (topics, callback) {
  this.loadMetadataForTopics([], (error, response) => {
    if (error) {
      return callback(error);
    }
    this.updateMetadatas(response);
    const missingTopics = _.difference(topics, Object.keys(this.topicMetadata));
    if (missingTopics.length === 0) {
      return callback(null);
    }
    callback(new errors.TopicsNotExistError(missingTopics));
  });
};

const encodeMessageSet = protocol.encodeMessageSet;
const Message = protocol.Message;

function compress (payloads, callback) {
  async.each(payloads, buildRequest, callback);

  function buildRequest (payload, cb) {
    const attributes = payload.attributes;
    const codec = getCodec(attributes);

    if (!codec) return cb(null);

    const innerSet = encodeMessageSet(payload.messages, 1);
    codec.encode(innerSet, function (err, message) {
      if (err) return cb(err);
      payload.messages = [new Message(0, attributes, payload.key, message)];
      cb(null);
    });
  }
}

function getSupportedForRequestType (broker, requestType) {
  assert(!_.isEmpty(broker.apiSupport), 'apiSupport is empty');
  const usable = broker.apiSupport[requestType].usable;

  const combo = apiMap[requestType][usable];
  return {
    encoder: combo[0],
    decoder: combo[1]
  };
}

KafkaClient.prototype.waitUntilReady = function (broker, callback) {
  let timeoutId = null;

  function onReady () {
    logger.debug('broker is now ready');
    clearTimeout(timeoutId);
    timeoutId = null;
    callback(null);
  }

  const timeout = this.options.requestTimeout;
  const readyEventName = broker.getReadyEventName();

  timeoutId = setTimeout(() => {
    this.removeListener(readyEventName, onReady);
    callback(new TimeoutError(`Request timed out after ${timeout}ms`));
  }, timeout);

  this.once(readyEventName, onReady);
};

KafkaClient.prototype.sendRequest = function (request, callback) {
  const payloads = this.payloadsByLeader(request.data.payloads);
  const longpolling = request.longpolling;

  const sendToBroker = async.ensureAsync((payload, leader, callback) => {
    const broker = this.brokerForLeader(leader, longpolling);
    if (!broker || !broker.isConnected()) {
      this.refreshBrokerMetadata();
      callback(new errors.BrokerNotAvailableError('Broker not available (sendRequest)'));
      return;
    }

    if (!broker.isReady()) {
      callback(new Error('Broker is not ready (apiSuppport is not set)'));
      return;
    }

    if (longpolling) {
      if (broker.socket.waiting) {
        callback(null);
        return;
      }
      broker.socket.waiting = true;
    }

    const correlationId = this.nextId();
    const coder = getSupportedForRequestType(broker, request.type);

    const encoder = request.data.args != null ? coder.encoder.apply(null, request.data.args) : coder.encoder;
    const decoder =
      request.data.decoderArgs != null ? coder.decoder.apply(null, request.data.decoderArgs) : coder.decoder;

    const requestData = encoder(this.clientId, correlationId, payload);

    if (request.data.requireAcks === 0) {
      broker.writeAsync(requestData);
      callback(null, { result: 'no ack' });
    } else {
      this.queueCallback(broker.socket, correlationId, [decoder, callback]);
      broker.write(requestData);
    }
  });

  const ensureBrokerReady = async.ensureAsync((leader, callback) => {
    const broker = this.brokerForLeader(leader, longpolling);
    if (!broker.isReady()) {
      logger.debug('missing apiSupport waiting until broker is ready...');
      this.waitUntilReady(broker, callback);
    } else {
      callback(null);
    }
  });

  async.mapValues(
    payloads,
    function (payload, leader, callback) {
      async.series(
        [
          function (callback) {
            ensureBrokerReady(leader, callback);
          },
          function (callback) {
            sendToBroker(payload, leader, callback);
          }
        ],
        function (error, results) {
          if (error) {
            return callback(error);
          }
          callback(null, _.last(results));
        }
      );
    },
    callback
  );
};

KafkaClient.prototype.leaderLessPayloads = function (payloads) {
  return _.filter(payloads, payload => !this.hasMetadata(payload.topic, payload.partition));
};

KafkaClient.prototype.verifyPayloadsHasLeaders = function (payloads, callback) {
  const leaderLessPayloads = this.leaderLessPayloads(payloads);

  if (leaderLessPayloads.length === 0) {
    return callback(null);
  }
  logger.debug('payloads has no leaders! Our metadata could be out of date try refreshingMetadata', leaderLessPayloads);
  this.refreshMetadata(_.map(leaderLessPayloads, 'topic'), error => {
    if (error) {
      return callback(error);
    }
    const payloadWithMissingLeaders = this.leaderLessPayloads(payloads);
    if (payloadWithMissingLeaders.length) {
      logger.error('leaders are still missing for %j', payloadWithMissingLeaders);
      callback(new errors.BrokerNotAvailableError('Could not find the leader'));
    } else {
      callback(null);
    }
  });
};

KafkaClient.prototype.wrapControllerCheckIfNeeded = function (encoder, decoder, encoderArgs, callback) {
  if (callback.isControllerWrapper) {
    return callback;
  }

  var hasBeenInvoked = false;

  const wrappedCallback = (error, result) => {
    if (error instanceof NotControllerError) {
      this.setControllerId(null);

      if (!hasBeenInvoked) {
        hasBeenInvoked = true;
        this.sendControllerRequest(encoder, decoder, encoderArgs, wrappedCallback);
        return;
      }
    }

    callback(error, result);
  };

  wrappedCallback.isControllerWrapper = true;

  return wrappedCallback;
};

KafkaClient.prototype.sendControllerRequest = function (encoder, decoder, encoderArgs, callback) {
  this.getController((error, controller) => {
    if (error) {
      return callback(error);
    }

    const originalArgs = _.clone(encoderArgs);
    const originalCallback = callback;
    const correlationId = this.nextId();
    encoderArgs.unshift(this.clientId, correlationId);
    const request = encoder.apply(null, encoderArgs);

    callback = this.wrapControllerCheckIfNeeded(encoder, decoder, originalArgs, originalCallback);

    this.queueCallback(controller.socket, correlationId, [decoder, callback]);
    controller.write(request);
  });
};

KafkaClient.prototype.sendFetchRequest = function (
  consumer,
  payloads,
  fetchMaxWaitMs,
  fetchMinBytes,
  maxTickMessages,
  callback
) {
  if (callback == null) {
    callback = _.noop;
  }

  async.series(
    [
      callback => {
        this.verifyPayloadsHasLeaders(payloads, callback);
      },
      callback => {
        const request = {
          type: 'fetch',
          longpolling: true,
          data: {
            payloads: payloads,
            args: [fetchMaxWaitMs, fetchMinBytes],
            decoderArgs: [this._createMessageHandler(consumer), maxTickMessages]
          }
        };

        this.sendRequest(request, callback);
      }
    ],
    callback
  );
};

KafkaClient.prototype.sendProduceRequest = function (payloads, requireAcks, ackTimeoutMs, callback) {
  if (this.closing) {
    callback(new errors.ClientIsClosing());
    return;
  }
  async.series(
    [
      function (callback) {
        logger.debug('compressing messages if needed');
        compress(payloads, callback);
      },
      callback => {
        this.verifyPayloadsHasLeaders(payloads, callback);
      },
      callback => {
        const request = {
          type: 'produce',
          data: {
            payloads: payloads,
            args: [requireAcks, ackTimeoutMs],
            requireAcks: requireAcks
          }
        };
        this.sendRequest(request, callback);
      }
    ],
    (err, result) => {
      if (err) {
        if (err.message === 'NotLeaderForPartition' || err.message === 'UnknownTopicOrPartition') {
          this.emit('brokersChanged');
        }
        callback(err);
      } else {
        callback(
          null,
          _.chain(result)
            .last()
            .reduce((accu, value) => _.merge(accu, value), {})
            .value()
        );
      }
    }
  );
};

module.exports = KafkaClient;
