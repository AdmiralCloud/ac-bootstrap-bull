'use strict'

const { expect } = require('chai')
const bullModule = require('../index')

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
