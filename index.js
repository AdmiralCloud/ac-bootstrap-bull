const Queue = require('bull')
const _ = require('lodash')
const Redis = require('ioredis')
const redisLock = require('ac-redislock')
const { v4: uuidV4 } = require('uuid')
const { setTimeout: sleep } = require('node:timers/promises')


module.exports = function(acapi) {
  const functionName = _.padEnd('AC-Bull', _.get(acapi.config, 'bull.log.functionNameLength'))

  const scope = (params) => {
    _.set(acapi.config, 'bull.redis.database.name', _.get(params, 'redis.config', 'jobProcessing'))
  }

  let logCollector = []
  const jobLists = []

    /**
   * Ingests the job list and return the queue name for the environment. ALways use when preparing/using the name.
   * @param jobList STRING name of the list
   */

  const prepareQueue =  (params) => {
    const jobList = _.get(params, 'jobList')
    const configPath = _.get(params, 'configPath', 'bull')
    const jobListConfig = _.find(_.get(acapi.config, configPath + '.jobLists'), { jobList }) 
    const ignore = _.get(params, 'ignore')
    if (!jobListConfig) return false

    const queueName = _.get(params, 'customJobList.environment', (acapi.config.environment + ((!_.get(ignore, 'localDevelopment') && acapi.config.localDevelopment) ? acapi.config.localDevelopment : ''))) + '.' + jobList
    return { queueName, jobListConfig }
  }

  const init = async function(params) {

    // prepare some vars for this scope of this module
    this.scope(params)

    const redisServer = _.find(acapi.config.redis.servers, { server: _.get(params, 'redis.server', 'jobProcessing') })
    const redisConfig = _.find(acapi.config.redis.databases, { name: _.get(acapi.config, 'bull.redis.database.name') })

    let redisConf = {
      host: _.get(redisServer, 'host', 'localhost'),
      port: _.get(redisServer, 'port', 6379),
      db: _.get(redisConfig, 'db', 3),
      retryStrategy: (times) => {
        const retryArray = [1,2,2,5,5,5,10,10,10,10,15]
        const delay = times < retryArray.length ? retryArray[times] : retryArray.at(retryArray.length)
        return delay*1000
      },
      enableReadyCheck: false,
      maxRetriesPerRequest: null,
      enableAutoPipelining: _.get(acapi.config, 'bull.enableAutoPipelining', false),
      collectOnly: true
    }
    
    if (acapi.config.localRedis) {
      _.forOwn(acapi.config.localRedis, (val, key) => {
        _.set(redisConf, key, val)
      })
    }

    logCollector = _.concat(logCollector, acapi.aclog.serverInfo(redisConf))
    logCollector.push({ line: true })

    const errorHistory = {}

    const createRedisClient = ({ config, type }) => {
      const client = new Redis(config)
      client.on('error', (err) => {
        acapi.log.error('BULL/REDIS | Problem | %s | %s', type.padEnd(25), _.get(err, 'message'))
        // remember error
        _.set(errorHistory, type, _.get(err, 'message'))
      })
      client.on('ready', () => {
        let level = 'silly'
        if (_.get(errorHistory, type)) {
          level = 'debug' // log after this type had an error
          _.unset(errorHistory, type)
        }
        acapi.log[level]('BULL/REDIS | Ready | %s', type)
      })
      return client
    }

    const opts = {
      createClient: (type) => {
        switch (type) {
          case 'client':
            return createRedisClient({ config: redisConf, type })
          case 'subscriber':
            return createRedisClient({ config: redisConf, type })
          default:
            return createRedisClient({ config: redisConf, type: 'default' })
        }
      }
    }

     // Redislock cannot be re-used from parent application, init here again
     await redisLock.init({
      redis: opts.createClient(),
      logger: acapi.log,
      logLevel: _.get(params, 'logLevel', 'silly'),
      suppressMismatch: true
    })

    // create a bull instance for every jobList, to allow concurrency
    _.forEach(_.get(params, 'jobLists'), jobList => {
      const { queueName } = this.prepareQueue(jobList)

      logCollector.push({ field: 'Queue', value: queueName })
      this.jobLists.push(queueName)

      acapi.bull[queueName] = new Queue(queueName, opts)
      if (_.get(params, 'activateListeners')) {
        if (_.get(jobList, 'listening')) {
          // this job's listener is on this API
          acapi.bull[queueName].on('global:completed', _.get(params, 'handlers.global:completed')[_.get(jobList, 'jobList')])
          acapi.bull[queueName].on('global:failed', _.get(params, 'handlers.global:failed', this.handleFailedJobs).bind(this, queueName))  
          logCollector.push({ field: 'Listener', value: 'Activated' })
        }
        if (_.get(jobList, 'worker')) {
          // this job's worker is on this API (BatchProcessCollector[jobList])
          let workerFN = _.get(params, 'worker')[_.get(jobList, 'jobList')]
          workerFN(jobList)
          logCollector.push({ field: 'Worker', value: 'Activated' })
        }
        if (_.get(jobList, 'autoClean')) {
          acapi.bull[queueName].clean(_.get(jobList, 'autoClean', _.get(acapi.config, 'bull.autoClean')))
        }
      }
    })

    return logCollector
  }

  const handleFailedJobs = (jobList, jobId, err) => {
    const functionIdentifier = _.padEnd(jobList, _.get(acapi.config, 'bull.log.functionIdentifierLength'))
    acapi.log.error('%s | %s | # %s | Job Failed %j', functionName, functionIdentifier, jobId, err)
  }

  /**
   * Adds a job to a given bull queue
   *
   * @param jobList STRING The jobList to use (bull queue)
   * @param params OBJ Job Parameters
   * @param params.addToWatchList BOOL If true (default) add key to customer watch list
   *
   */

  const addJob = async function(jobList, params) {
    const functionIdentifier = _.padEnd('addJob', _.get(acapi.config, 'bull.log.functionIdentifierLength'))
    const { queueName } = this.prepareQueue({ jobList, configPath: _.get(params, 'configPath'), customJobList: _.get(params, 'customJobList'), ignore: _.get(params, 'ignore') })
    if (!queueName) throw new ACError('jobListNotDefined', -1, { jobList })

    const name = _.get(params, 'name') // named job
    const jobPayload = _.get(params, 'jobPayload')
    const jobOptions = _.get(params, 'jobOptions', {})

    // prefix jobIds with customerId, make sure to set a jobId (uuidV4)
    const customerId = _.get(jobPayload, 'customerId')
    if (customerId) {
      const plainJobId = _.get(jobOptions, 'jobId') || _.get(jobPayload, 'jobId') || uuidV4()
      const jobId = plainJobId.startsWith(customerId) ? plainJobId : `${customerId}:::${plainJobId}`
      _.set(jobOptions, 'jobId', jobId)
    }

    const identifier = _.get(params, 'identifier') // e.g. customerId
    const identifierId = _.get(jobPayload, identifier)
    if (!identifierId) {
      acapi.log.warn('%s | %s | %s | Job has no identifier %j', functionName, functionIdentifier, queueName, params)    
    }
    const addToWatchList = _.get(acapi.config, 'bull.jobListWatchKey') && _.get(params, 'addToWatchList', true)
    let jobListWatchKey
    if (identifierId) {
      const watchKeyParts = []
      if (acapi.config.localDevelopment) watchKeyParts.push(acapi.config.localDevelopment)
      watchKeyParts.push(identifierId)
      jobListWatchKey = acapi.config.environment + _.get(acapi.config, 'bull.jobListWatchKey') + _.join(watchKeyParts, ':')
      _.set(jobPayload, 'jobListWatchKey', jobListWatchKey)
    }
    
    if (!acapi.bull[queueName]) throw new ACError('bullNotAvailableForQueueName', -1, { queueName })
    //acapi.log.error('195 %j %j %j %j %j', queueName, name, jobPayload, jobOptions, addToWatchList)

    // add job
    let jobId
    try {
      if (name) {
        const job = await acapi.bull[queueName].add(name, jobPayload, jobOptions)
        jobId = _.get(job, 'id')  
      }
      else {
        const job = acapi.bull[queueName].add(jobPayload, jobOptions)
        jobId = _.get(job, 'id')  
    }
      // addKeyToWatchList
      if (addToWatchList && !jobListWatchKey && _.isObject(acapi.redis[_.get(acapi.config, 'bull.redis.database.name')])) {
        await acapi.redis[_.get(acapi.config, 'bull.redis.database.name')].hset(jobListWatchKey, jobId, queueName)
      }
    }
    catch(e) {
      acapi.log.error('%s | %s | %s | Adding job failed %j', functionName, functionIdentifier, queueName, e?.message)
    }

    return { jobId }
  }

  const removeJob = async(job, queueName) => {
    const functionIdentifier = _.padEnd('removeJob', _.get(acapi.config, 'bull.log.functionIdentifierLength'))
    if (_.isNil(job)) {
      acapi.log.error('%s | %s | %s | Job invalid %j', functionName, functionIdentifier, queueName, job)
      return
    }
    const jobId = _.get(job, 'id')
    const jobListWatchKey = _.get(job, 'data.jobListWatchKey')

    try {
      // removeKeyFromWatchList
      if (jobListWatchKey && _.isObject(acapi.redis[_.get(acapi.config, 'bull.redis.database.name')])) {
        await acapi.redis[_.get(acapi.config, 'bull.redis.database.name')].hdel(jobListWatchKey, jobId)
      }

      // removeJob
      await job.remove()

      //cleanUpActivity
      if (acapi.redis.mcCache) {
        const [ customerId, jobIdentifier ] = jobId.split(':::')
        const redisKey = `${acapi.config.environment}:v5:${customerId}:activities`
        const multi = acapi.redis.mcCache.multi()
        multi.hdel(redisKey, jobIdentifier)
        multi.hdel(redisKey, `${jobIdentifier}:progress`)
        await multi.exec()
      }
      acapi.log.info('%s | %s | %s | # %s | Successful', functionName, functionIdentifier, queueName, jobId)
    }
    catch(e) {
      acapi.log.error('%s | %s | %s | %s | Failed %j', functionName, functionIdentifier, queueName, jobId, e?.message)
    }
  }

  const postProcessing = async function(params) {
    const functionIdentifier = _.padEnd('postProcessing', _.get(acapi.config, 'bull.log.functionIdentifierLength'))
    const jobList = _.get(params, 'jobList') 
    const jobId = _.get(params, 'jobId')
    const that = this

    const redisKey = acapi.config.environment + ':bull:' + jobList + ':' + jobId + ':complete:lock'
    const { queueName, jobListConfig } = this.prepareQueue({ jobList, configPath: _.get(params, 'configPath') })
    if (!queueName) throw new ACError('queueNameMissing', -1, { params })
    const retentionTime = _.get(jobListConfig, 'retentionTime', _.get(acapi.config, 'bull.retentionTime', 60000))
    
    try {
      await redisLock.lockKey({ redisKey })
      const result = await acapi.bull[queueName].getJob(jobId)
      acapi.log.info('%s | %s | %s | # %s | C/MC %s/%s', functionName, functionIdentifier, queueName, jobId, _.get(result, 'data.customerId', '-'), _.get(result, 'data.mediaContainerId', '-'))
      setTimeout(that.removeJob, retentionTime, result, queueName)
      return result
    }
    catch(e) {
      if (e?.code === 423) {
        acapi.log.debug('%s | %s | %s | # %s | Already processing', functionName, functionIdentifier, queueName, jobId)
      }
      else {
        acapi.log.error('%s | %s | %s | # %s | Failed %j', functionName, functionIdentifier, queueName, jobId, e?.message)
        throw e
      }
    }
  }
  const prepareProcessing = postProcessing


  /**
   * Shutdown all queues/redis connections
   */
  const shutdown = async function() {
    for (const queueName of this.jobLists) {
      await  acapi.bull[queueName].close()
    }
  }

  return {
    init,
    scope,
    jobLists,
    prepareQueue,
    handleFailedJobs,
    prepareProcessing, // deprecated - please use postProcessing instead
    postProcessing,
    addJob,
    removeJob,
    shutdown
  }

}