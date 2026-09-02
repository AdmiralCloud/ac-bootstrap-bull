const Queue = require('bull')
const Redis = require('ioredis')
const redisLock = require('ac-redislock')



module.exports = function(acapi) {
  const functionName = 'AC-Bull'.padEnd(acapi.config?.bull?.log?.functionNameLength)

  const scope = (params) => {
    if (!acapi.config.bull.redis) acapi.config.bull.redis = {}
    if (!acapi.config.bull.redis.database) acapi.config.bull.redis.database = {}
    acapi.config.bull.redis.database.name = params?.redis?.config ?? 'jobProcessing'
  }

  let logCollector = []
  const jobLists = []
  let myLock

    /**
   * Ingests the job list and return the queue name for the environment. Always use when preparing/using the name.
   * @param jobList STRING name of the list
   */

  const prepareQueue =  (params) => {
    const jobList = params?.jobList
    const configPath = params?.configPath ?? 'bull'
    const jobListConfig = (acapi.config[configPath]?.jobLists ?? []).find(item => item.jobList === jobList)
    const ignore = params?.ignore
    if (!jobListConfig) { return false }

    const env = params?.customJobList?.environment ?? (acapi.config.environment + ((!ignore?.localDevelopment && acapi.config.localDevelopment) ? acapi.config.localDevelopment : ''))
    const queueName = env + '.' + jobList
    return { queueName, jobListConfig }
  }

  const init = async function(params) {

    // prepare some vars for this scope of this module
    this.scope(params)

    const redisServer = acapi.config.redis.servers.find(s => s.server === (params?.redis?.server ?? 'jobProcessing'))
    const redisConfig = acapi.config.redis.databases.find(d => d.name === acapi.config.bull?.redis?.database?.name)

    const redisConf = {
      host: redisServer?.host ?? 'localhost',
      port: redisServer?.port ?? 6379,
      db: redisConfig?.db ?? 3,
      retryStrategy: (times) => {
        const retryArray = [1,2,2,5,5,5,10,10,10,10,15]
        const delay = times < retryArray.length ? retryArray[times] : retryArray.at(-1)
        return delay*1000
      },
      enableReadyCheck: false,
      maxRetriesPerRequest: null,
      enableAutoPipelining: acapi.config?.bull?.enableAutoPipelining ?? false,
      collectOnly: true
    }

    if (acapi.config.localRedis) {
      Object.entries(acapi.config.localRedis).forEach(([key, val]) => {
        redisConf[key] = val
      })
    }

    logCollector = [...logCollector, ...acapi.aclog.serverInfo(redisConf)]
    logCollector.push({ line: true })

    const errorHistory = {}

    const createRedisClient = ({ config, type }) => {
      const client = new Redis(config)
      client.on('error', (err) => {
        acapi.log.error('BULL/REDIS | Problem | %s | %s', type.padEnd(25), err?.message)
        errorHistory[type] = err?.message
      })
      client.on('ready', () => {
        let level = 'silly'
        if (errorHistory[type]) {
          level = 'debug' // log after this type had an error
          delete errorHistory[type]
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
    myLock = redisLock.create()
    await myLock.init({
      redis: opts.createClient(),
      logger: acapi.log,
      logLevel: params?.logLevel ?? 'silly',
      suppressMismatch: true
    })

    // create a bull instance for every jobList, to allow concurrency
    ;(params?.jobLists ?? []).forEach(jobList => {
      const { queueName } = this.prepareQueue(jobList)

      logCollector.push({ field: 'Queue', value: queueName })
      this.jobLists.push(queueName)

      acapi.bull[queueName] = new Queue(queueName, opts)
      if (params?.activateListeners) {
        if (jobList?.listening) {
          // this job's listener is on this API
          acapi.bull[queueName].on('global:completed', params?.handlers?.['global:completed']?.[jobList?.jobList])
          acapi.bull[queueName].on('global:failed', (params?.handlers?.['global:failed'] ?? this.handleFailedJobs).bind(this, queueName))
          logCollector.push({ field: 'Listener', value: 'Activated' })
        }
        if (jobList?.worker) {
          // this job's worker is on this API (BatchProcessCollector[jobList])
          const workerFN = params?.worker?.[jobList?.jobList]
          workerFN(jobList)
          logCollector.push({ field: 'Worker', value: 'Activated' })
        }
        if (jobList?.autoClean) {
          acapi.bull[queueName].clean(jobList?.autoClean ?? acapi.config?.bull?.autoClean)
        }
      }
    })

    return logCollector
  }

  const handleFailedJobs = (jobList, jobId, err) => {
    const functionIdentifier = jobList.padEnd(acapi.config?.bull?.log?.functionIdentifierLength)
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
    const functionIdentifier = 'addJob'.padEnd(acapi.config?.bull?.log?.functionIdentifierLength)
    const { queueName } = this.prepareQueue({ jobList, configPath: params?.configPath, customJobList: params?.customJobList, ignore: params?.ignore })
    if (!queueName) { throw new ACError('jobListNotDefined', -1, { jobList }) }

    const name = params?.name // named job
    const jobPayload = params?.jobPayload
    const jobOptions = params?.jobOptions ?? {}

    // prefix jobIds with customerId, make sure to set a jobId (uuidV4)
    const customerId = jobPayload?.customerId
    if (customerId) {
      const plainJobId = jobOptions?.jobId || jobPayload?.jobId || crypto.randomUUID()
      const jobId = plainJobId.startsWith(customerId) ? plainJobId : `${customerId}:::${plainJobId}`
      jobOptions.jobId = jobId
    }

    const identifier = params?.identifier // e.g. customerId
    const identifierId = jobPayload?.[identifier]
    if (!identifierId) {
      acapi.log.warn('%s | %s | %s | Job has no identifier %j', functionName, functionIdentifier, queueName, params)
    }
    const addToWatchList = acapi.config?.bull?.jobListWatchKey && (params?.addToWatchList ?? true)
    let jobListWatchKey
    if (identifierId) {
      const watchKeyParts = []
      if (acapi.config.localDevelopment) watchKeyParts.push(acapi.config.localDevelopment)
      watchKeyParts.push(identifierId)
      jobListWatchKey = acapi.config.environment + acapi.config?.bull?.jobListWatchKey + watchKeyParts.join(':')
      jobPayload.jobListWatchKey = jobListWatchKey
    }

    if (!acapi.bull[queueName]) { throw new ACError('bullNotAvailableForQueueName', -1, { queueName }) }

    // add job
    let jobId
    try {
      if (name) {
        const job = await acapi.bull[queueName].add(name, jobPayload, jobOptions)
        jobId = job?.id
      }
      else {
        const job = await acapi.bull[queueName].add(jobPayload, jobOptions)
        jobId = job?.id
      }
      // addKeyToWatchList
      if (addToWatchList && jobListWatchKey && typeof acapi.redis[acapi.config?.bull?.redis?.database?.name] === 'object') {
        await acapi.redis[acapi.config?.bull?.redis?.database?.name].hset(jobListWatchKey, jobId, queueName)
      }
    }
    catch(e) {
      acapi.log.error('%s | %s | %s | Adding job failed %j', functionName, functionIdentifier, queueName, e?.message)
    }

    return { jobId }
  }

  const removeJob = async(job, queueName) => {
    const functionIdentifier = 'removeJob'.padEnd(acapi.config?.bull?.log?.functionIdentifierLength)
    if (job == null) {
      acapi.log.error('%s | %s | %s | Job invalid %j', functionName, functionIdentifier, queueName, job)
      return
    }
    const jobId = job.id
    const jobListWatchKey = job?.data?.jobListWatchKey

    try {
      // removeKeyFromWatchList
      if (jobListWatchKey && typeof acapi.redis[acapi.config?.bull?.redis?.database?.name] === 'object') {
        await acapi.redis[acapi.config?.bull?.redis?.database?.name].hdel(jobListWatchKey, jobId)
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
    const functionIdentifier = 'postProcessing'.padEnd(acapi.config?.bull?.log?.functionIdentifierLength)
    const jobList = params?.jobList
    const jobId = params?.jobId
    const that = this

    const redisKey = acapi.config.environment + ':bull:' + jobList + ':' + jobId + ':complete:lock'
    const { queueName, jobListConfig } = this.prepareQueue({ jobList, configPath: params?.configPath })
    if (!queueName) { throw new ACError('queueNameMissing', -1, { params }) }
    const retentionTime = jobListConfig?.retentionTime ?? acapi.config?.bull?.retentionTime ?? 60000

    try {
      await myLock.lockKey({ redisKey })
      const result = await acapi.bull[queueName].getJob(jobId)
      acapi.log.info('%s | %s | %s | # %s | C/MC %s/%s', functionName, functionIdentifier, queueName, jobId, result?.data?.customerId ?? '-', result?.data?.mediaContainerId ?? '-')
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
