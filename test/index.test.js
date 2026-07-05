'use strict'

const { expect } = require('chai')
const bullModule = require('../index')

// ------------------------------------------------------------------
// require.cache mocking helpers for init / postProcessing tests
// ------------------------------------------------------------------

let createdRedisClients = []

class MockRedis {
  constructor() {
    this._listeners = {}
    createdRedisClients.push(this)
  }
  on(event, cb) { this._listeners[event] = cb; return this }
  trigger(event, ...args) { if (this._listeners[event]) this._listeners[event](...args) }
}

class MockQueue {
  constructor(name, opts) {
    this.name = name
    this._listeners = {}
    // exercise createClient paths (client / subscriber / default)
    if (opts && opts.createClient) {
      opts.createClient('client')
      opts.createClient('subscriber')
      opts.createClient('other')
    }
  }
  on(event, cb) { this._listeners[event] = cb; return this }
  clean() {}
}

const mockRedisLock = {
  init: async () => {},
  lockKey: async () => {},
  create: () => ({
    init: async () => {},
    lockKey: (...args) => mockRedisLock.lockKey(...args)
  })
}

function withMockedDeps(fn) {
  const bullKey      = require.resolve('bull')
  const redisKey     = require.resolve('ioredis')
  const lockKey      = require.resolve('ac-redislock')
  const indexKey     = require.resolve('../index')

  const savedBull    = require.cache[bullKey]
  const savedRedis   = require.cache[redisKey]
  const savedLock    = require.cache[lockKey]

  require.cache[bullKey]  = { exports: MockQueue }
  require.cache[redisKey] = { exports: MockRedis }
  require.cache[lockKey]  = { exports: mockRedisLock }
  delete require.cache[indexKey]
  const mocked = require('../index')

  try {
    return fn(mocked)
  } finally {
    require.cache[bullKey]  = savedBull
    require.cache[redisKey] = savedRedis
    require.cache[lockKey]  = savedLock
    delete require.cache[indexKey]
  }
}

// ACError is a global in the consuming application
global.ACError = class ACError extends Error {
  constructor(code, status, meta) {
    super(code)
    this.code = code
    this.status = status
    this.meta = meta
  }
}

const createAcapi = (overrides = {}) => ({
  config: {
    environment: 'test',
    bull: {
      log: { functionNameLength: 20, functionIdentifierLength: 20 },
      jobLists: [
        { jobList: 'myQueue' },
        { jobList: 'otherQueue', retentionTime: 5000 }
      ],
      ...(overrides.bull || {})
    },
    redis: {
      servers: [{ server: 'jobProcessing', host: 'localhost', port: 6379 }],
      databases: [{ name: 'jobProcessing', db: 3 }]
    },
    ...(overrides.config || {})
  },
  log: {
    error: () => {},
    warn: () => {},
    info: () => {},
    debug: () => {},
    silly: () => {}
  },
  aclog: { serverInfo: () => [] },
  bull: {},
  redis: {},
  ...overrides
})

// ------------------------------------------------------------------
// helpers
// ------------------------------------------------------------------

const makeMockQueue = (addResult = { id: 'job-123' }) => ({
  add: async (...args) => addResult,
  close: async () => {}
})

// ------------------------------------------------------------------
// tests
// ------------------------------------------------------------------

describe('ac-bootstrap-bull', () => {

  describe('prepareQueue', () => {
    let acapi, bull

    beforeEach(() => {
      acapi = createAcapi()
      bull = bullModule(acapi)
    })

    it('returns false when jobList is not in config', () => {
      expect(bull.prepareQueue({ jobList: 'nonexistent' })).to.be.false
    })

    it('builds queueName as <environment>.<jobList>', () => {
      const { queueName } = bull.prepareQueue({ jobList: 'myQueue' })
      expect(queueName).to.equal('test.myQueue')
    })

    it('appends localDevelopment suffix to queueName', () => {
      acapi.config.localDevelopment = '-dev'
      const { queueName } = bull.prepareQueue({ jobList: 'myQueue' })
      expect(queueName).to.equal('test-dev.myQueue')
    })

    it('skips localDevelopment suffix when ignore.localDevelopment is true', () => {
      acapi.config.localDevelopment = '-dev'
      const { queueName } = bull.prepareQueue({ jobList: 'myQueue', ignore: { localDevelopment: true } })
      expect(queueName).to.equal('test.myQueue')
    })

    it('uses customJobList.environment instead of acapi.config.environment', () => {
      const { queueName } = bull.prepareQueue({ jobList: 'myQueue', customJobList: { environment: 'staging' } })
      expect(queueName).to.equal('staging.myQueue')
    })

    it('returns the matching jobListConfig', () => {
      const { jobListConfig } = bull.prepareQueue({ jobList: 'myQueue' })
      expect(jobListConfig).to.deep.equal({ jobList: 'myQueue' })
    })

    it('uses a custom configPath to look up jobLists', () => {
      acapi.config.custom = { jobLists: [{ jobList: 'myQueue' }] }
      const { queueName } = bull.prepareQueue({ jobList: 'myQueue', configPath: 'custom' })
      expect(queueName).to.equal('test.myQueue')
    })
  })

  // ------------------------------------------------------------------

  describe('scope', () => {
    let acapi, bull

    beforeEach(() => {
      acapi = createAcapi()
      bull = bullModule(acapi)
    })

    it('sets bull.redis.database.name from params.redis.config', () => {
      bull.scope({ redis: { config: 'myDatabase' } })
      expect(acapi.config.bull.redis.database.name).to.equal('myDatabase')
    })

    it('defaults to "jobProcessing" when redis.config is not provided', () => {
      bull.scope({})
      expect(acapi.config.bull.redis.database.name).to.equal('jobProcessing')
    })
  })

  // ------------------------------------------------------------------

  describe('handleFailedJobs', () => {
    it('logs an error', () => {
      let errorLogged = false
      const acapi = createAcapi()
      acapi.log.error = () => { errorLogged = true }
      const bull = bullModule(acapi)

      bull.handleFailedJobs('test.myQueue', 'job-1', new Error('timeout'))
      expect(errorLogged).to.be.true
    })
  })

  // ------------------------------------------------------------------

  describe('addJob', () => {
    const QUEUE_NAME = 'test.myQueue'
    let acapi, bull

    beforeEach(() => {
      acapi = createAcapi()
      bull = bullModule(acapi)
      acapi.bull[QUEUE_NAME] = makeMockQueue()
    })

    it('throws ACError when jobList is not configured', async () => {
      try {
        await bull.addJob('unknownQueue', {})
        expect.fail('expected ACError')
      } catch (e) {
        expect(e).to.be.instanceOf(ACError)
        expect(e.code).to.equal('jobListNotDefined')
      }
    })

    it('throws ACError when bull queue is not initialized', async () => {
      delete acapi.bull[QUEUE_NAME]
      try {
        await bull.addJob('myQueue', { jobPayload: {} })
        expect.fail('expected ACError')
      } catch (e) {
        expect(e).to.be.instanceOf(ACError)
        expect(e.code).to.equal('bullNotAvailableForQueueName')
      }
    })

    it('prefixes jobId with customerId when customerId is present', async () => {
      let capturedOptions
      acapi.bull[QUEUE_NAME].add = async (payload, options) => {
        capturedOptions = options
        return { id: options.jobId }
      }

      await bull.addJob('myQueue', { jobPayload: { customerId: 'cust-1' } })
      expect(capturedOptions.jobId).to.match(/^cust-1:::/)
    })

    it('does not double-prefix when jobId already starts with customerId', async () => {
      let capturedOptions
      acapi.bull[QUEUE_NAME].add = async (payload, options) => {
        capturedOptions = options
        return { id: options.jobId }
      }

      await bull.addJob('myQueue', {
        jobPayload: { customerId: 'cust-1' },
        jobOptions: { jobId: 'cust-1:::existing-id' }
      })
      expect(capturedOptions.jobId).to.equal('cust-1:::existing-id')
    })

    it('returns { jobId } after adding a named job', async () => {
      acapi.bull[QUEUE_NAME].add = async (name, payload, options) => ({ id: 'named-job-id' })
      const result = await bull.addJob('myQueue', { name: 'myJobName', jobPayload: {} })
      expect(result).to.deep.equal({ jobId: 'named-job-id' })
    })

    it('returns { jobId } after adding an unnamed job', async () => {
      acapi.bull[QUEUE_NAME].add = async (payload, options) => ({ id: 'unnamed-job-id' })
      const result = await bull.addJob('myQueue', { jobPayload: {} })
      expect(result).to.deep.equal({ jobId: 'unnamed-job-id' })
    })

    it('logs warn when jobPayload has no identifier value', async () => {
      let warnLogged = false
      acapi.log.warn = () => { warnLogged = true }
      bull = bullModule(acapi)
      acapi.bull[QUEUE_NAME] = makeMockQueue()
      await bull.addJob('myQueue', { jobPayload: {}, identifier: 'customerId' })
      expect(warnLogged).to.be.true
    })

    it('logs error and returns { jobId: undefined } when queue.add throws', async () => {
      let errorLogged = false
      acapi.log.error = () => { errorLogged = true }
      bull = bullModule(acapi)
      acapi.bull[QUEUE_NAME] = { add: async () => { throw new Error('Redis down') }, close: async () => {} }
      const result = await bull.addJob('myQueue', { name: 'myJob', jobPayload: {} })
      expect(errorLogged).to.be.true
      expect(result).to.deep.equal({ jobId: undefined })
    })

    it('assembles jobListWatchKey and sets it in jobPayload when identifierId is present', async () => {
      let capturedPayload
      acapi.bull[QUEUE_NAME].add = async (payload, options) => {
        capturedPayload = payload
        return { id: 'job-xyz' }
      }
      await bull.addJob('myQueue', {
        jobPayload: { customerId: 'cust-99' },
        identifier: 'customerId'
      })
      expect(capturedPayload).to.have.property('jobListWatchKey')
      expect(capturedPayload.jobListWatchKey).to.include('cust-99')
    })

    it('includes localDevelopment prefix in jobListWatchKey', async () => {
      acapi.config.localDevelopment = '-dev'
      let capturedPayload
      acapi.bull['test-dev.myQueue'] = {
        add: async (payload, options) => { capturedPayload = payload; return { id: 'job-xyz' } },
        close: async () => {}
      }
      await bull.addJob('myQueue', {
        jobPayload: { customerId: 'cust-99' },
        identifier: 'customerId'
      })
      expect(capturedPayload.jobListWatchKey).to.include('-dev')
    })
  })

  // ------------------------------------------------------------------

  describe('removeJob', () => {
    let acapi, bull

    beforeEach(() => {
      acapi = createAcapi()
      bull = bullModule(acapi)
    })

    it('logs an error and returns early when job is null', async () => {
      let errorLogged = false
      acapi.log.error = () => { errorLogged = true }
      bull = bullModule(acapi)

      await bull.removeJob(null, 'test.myQueue')
      expect(errorLogged).to.be.true
    })

    it('calls job.remove() on a valid job', async () => {
      let removed = false
      const mockJob = {
        id: 'job-abc',
        data: {},
        remove: async () => { removed = true }
      }

      await bull.removeJob(mockJob, 'test.myQueue')
      expect(removed).to.be.true
    })

    it('logs success after removing a valid job', async () => {
      let infoLogged = false
      acapi.log.info = () => { infoLogged = true }
      bull = bullModule(acapi)

      const mockJob = { id: 'job-abc', data: {}, remove: async () => {} }
      await bull.removeJob(mockJob, 'test.myQueue')
      expect(infoLogged).to.be.true
    })

    it('logs error when job.remove() throws', async () => {
      let errorLogged = false
      acapi.log.error = () => { errorLogged = true }
      bull = bullModule(acapi)

      const mockJob = {
        id: 'job-abc',
        data: {},
        remove: async () => { throw new Error('remove failed') }
      }
      await bull.removeJob(mockJob, 'test.myQueue')
      expect(errorLogged).to.be.true
    })

    it('cleans up mcCache entries when mcCache is available', async () => {
      const hdels = []
      const mockMulti = {
        hdel: (...args) => { hdels.push(args); return mockMulti },
        exec: async () => {}
      }
      acapi = createAcapi()
      acapi.redis = { mcCache: { multi: () => mockMulti } }
      bull = bullModule(acapi)

      const mockJob = {
        id: 'cust-1:::job-abc',
        data: {},
        remove: async () => {}
      }
      await bull.removeJob(mockJob, 'test.myQueue')
      expect(hdels).to.have.length(2)
    })

    it('calls redis hdel when job has a jobListWatchKey and redis db is available', async () => {
      const hdelCalls = []
      acapi = createAcapi()
      acapi.redis = {
        jobProcessing: { hdel: async (...args) => hdelCalls.push(args) }
      }
      bull = bullModule(acapi)
      bull.scope({}) // sets bull.redis.database.name = 'jobProcessing'

      const mockJob = {
        id: 'job-abc',
        data: { jobListWatchKey: 'test:watchkey' },
        remove: async () => {}
      }
      await bull.removeJob(mockJob, 'test.myQueue')
      expect(hdelCalls).to.have.length(1)
      expect(hdelCalls[0]).to.deep.equal(['test:watchkey', 'job-abc'])
    })
  })

  // ------------------------------------------------------------------

  // ------------------------------------------------------------------

  describe('init (mocked deps)', () => {
    beforeEach(() => { createdRedisClients = [] })

    it('initializes queues and returns logCollector', async () => {
      await withMockedDeps(async (mod) => {
        const acapi = createAcapi()
        const bull = mod(acapi)
        const result = await bull.init.call(bull, {
          jobLists: [{ jobList: 'myQueue' }]
        })
        expect(result).to.be.an('array')
        expect(bull.jobLists).to.include('test.myQueue')
        expect(acapi.bull['test.myQueue']).to.be.instanceOf(MockQueue)
      })
    })

    it('applies localRedis overrides to redisConf', async () => {
      await withMockedDeps(async (mod) => {
        const acapi = createAcapi()
        acapi.config.localRedis = { db: 7 }
        const bull = mod(acapi)
        // should not throw; localRedis path is executed
        await bull.init.call(bull, { jobLists: [{ jobList: 'myQueue' }] })
      })
    })

    it('fires Redis error event and logs it', async () => {
      await withMockedDeps(async (mod) => {
        const acapi = createAcapi()
        let errorLogged = false
        acapi.log.error = () => { errorLogged = true }
        const bull = mod(acapi)
        await bull.init.call(bull, { jobLists: [{ jobList: 'myQueue' }] })
        // trigger error on first created client (the redisLock client)
        createdRedisClients[0].trigger('error', new Error('conn refused'))
        expect(errorLogged).to.be.true
      })
    })

    it('fires Redis ready event (first time → silly, after error → debug)', async () => {
      await withMockedDeps(async (mod) => {
        const acapi = createAcapi()
        const levels = []
        acapi.log.silly = () => levels.push('silly')
        acapi.log.debug = () => levels.push('debug')
        const bull = mod(acapi)
        await bull.init.call(bull, { jobLists: [{ jobList: 'myQueue' }] })
        const client = createdRedisClients[0]
        client.trigger('ready')                         // first time → silly
        client.trigger('error', new Error('conn lost'))
        client.trigger('ready')                         // after error → debug
        expect(levels).to.include('silly')
        expect(levels).to.include('debug')
      })
    })

    it('activates listener on a queue', async () => {
      await withMockedDeps(async (mod) => {
        const acapi = createAcapi()
        const bull = mod(acapi)
        let listenerAttached = false
        MockQueue.prototype.on = function(event, cb) {
          if (event === 'global:completed') listenerAttached = true
          return this
        }
        await bull.init.call(bull, {
          activateListeners: true,
          jobLists: [{ jobList: 'myQueue', listening: true }],
          handlers: { 'global:completed': { myQueue: () => {} } }
        })
        expect(listenerAttached).to.be.true
        MockQueue.prototype.on = function(e, cb) { return this }
      })
    })

    it('activates a worker function for a queue', async () => {
      await withMockedDeps(async (mod) => {
        const acapi = createAcapi()
        const bull = mod(acapi)
        let workerCalled = false
        await bull.init.call(bull, {
          activateListeners: true,
          jobLists: [{ jobList: 'myQueue', worker: true }],
          worker: { myQueue: () => { workerCalled = true } }
        })
        expect(workerCalled).to.be.true
      })
    })

    it('calls queue.clean when autoClean is set', async () => {
      await withMockedDeps(async (mod) => {
        const acapi = createAcapi()
        const bull = mod(acapi)
        let cleanCalled = false
        MockQueue.prototype.clean = function() { cleanCalled = true }
        await bull.init.call(bull, {
          activateListeners: true,
          jobLists: [{ jobList: 'myQueue', autoClean: 5000 }]
        })
        expect(cleanCalled).to.be.true
        delete MockQueue.prototype.clean
      })
    })
  })

  // ------------------------------------------------------------------

  describe('postProcessing (mocked deps)', () => {
    it('throws ACError when jobList is not configured', async () => {
      const acapi = createAcapi()
      const bull = bullModule(acapi)
      try {
        await bull.postProcessing.call(bull, { jobList: 'nonexistent', jobId: 'job-1' })
        expect.fail('expected ACError')
      } catch (e) {
        expect(e).to.be.instanceOf(ACError)
        expect(e.code).to.equal('queueNameMissing')
      }
    })

    it('retrieves a job and schedules its removal', async () => {
      await withMockedDeps(async (mod) => {
        const acapi = createAcapi()
        const bull = mod(acapi)
        await bull.init.call(bull, { jobLists: [{ jobList: 'myQueue' }] })
        const mockJob = { id: 'job-1', data: { customerId: 'c1', mediaContainerId: 'm1' } }
        acapi.bull['test.myQueue'] = { getJob: async () => mockJob }
        const result = await bull.postProcessing.call(bull, { jobList: 'myQueue', jobId: 'job-1' })
        expect(result).to.equal(mockJob)
      })
    })

    it('handles 423 lock contention silently', async () => {
      await withMockedDeps(async (mod) => {
        const lockErr = Object.assign(new Error('locked'), { code: 423 })
        mockRedisLock.lockKey = async () => { throw lockErr }
        const acapi = createAcapi()
        let debugLogged = false
        acapi.log.debug = () => { debugLogged = true }
        const bull = mod(acapi)
        await bull.init.call(bull, { jobLists: [{ jobList: 'myQueue' }] })
        const result = await bull.postProcessing.call(bull, { jobList: 'myQueue', jobId: 'job-1' })
        expect(result).to.be.undefined
        expect(debugLogged).to.be.true
        mockRedisLock.lockKey = async () => {}
      })
    })

    it('rethrows non-423 errors from postProcessing', async () => {
      await withMockedDeps(async (mod) => {
        mockRedisLock.lockKey = async () => { throw new Error('unexpected') }
        const acapi = createAcapi()
        const bull = mod(acapi)
        await bull.init.call(bull, { jobLists: [{ jobList: 'myQueue' }] })
        try {
          await bull.postProcessing.call(bull, { jobList: 'myQueue', jobId: 'job-1' })
          expect.fail('expected error')
        } catch (e) {
          expect(e.message).to.equal('unexpected')
        }
        mockRedisLock.lockKey = async () => {}
      })
    })
  })

  // ------------------------------------------------------------------

  describe('shutdown', () => {
    it('closes all registered queues', async () => {
      const acapi = createAcapi()
      const bull = bullModule(acapi)
      const closedQueues = []

      // manually populate jobLists as init would
      bull.jobLists.push('test.myQueue', 'test.otherQueue')
      acapi.bull['test.myQueue']   = { close: async () => closedQueues.push('test.myQueue') }
      acapi.bull['test.otherQueue'] = { close: async () => closedQueues.push('test.otherQueue') }

      await bull.shutdown()
      expect(closedQueues).to.include.members(['test.myQueue', 'test.otherQueue'])
    })
  })

})
