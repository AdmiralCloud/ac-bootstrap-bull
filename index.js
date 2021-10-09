const Queue = require('bull')
const _ = require('lodash')
const Redis = require('ioredis')
const async = require('async')
const redisLock = require('ac-redislock')


module.exports = function(acapi) {
  const functionName = _.padEnd('AC-Bull', _.get(acapi.config, 'bull.log.functionNameLength'))

  const scope = (params) => {
    _.set(acapi.config, 'bull.redis.database.name', _.get(params, 'redis.config', 'jobProcessing'))
  }

  const jobLists = []

    /**
   * Ingests the job list and return the queue name for the environment. ALways use when preparing/using the name.
   * @param jobList STRING name of the list
   */

  const prepareQueue =  (params) => {
    const jobList = _.get(params, 'jobList')
    const configPath = _.get(params, 'configPath', 'bull')
    const jobListConfig = _.find(_.get(acapi.config, configPath + '.jobLists'), { jobList }) 
    if (!jobListConfig) return false

    const queueName = _.get(params, 'customJobList.environment', (acapi.config.environment + (acapi.config.localDevelopment ? acapi.config.localDevelopment : ''))) + '.' + jobList
    return { queueName, jobListConfig }
  }

  const init = function(params, cb) {
    acapi.aclog.headline({ headline: 'bull' })

    // prepare some vars for this scope of this module
    this.scope(params)

    const redisServer = _.find(acapi.config.redis.servers, { server: _.get(params, 'redis.server', 'jobProcessing') })
    const redisConfig = _.find(acapi.config.redis.databases, { name: _.get(acapi.config, 'bull.redis.database.name') })
    const port = acapi.config.localRedis ? 6379 : _.get(redisServer, 'port')

    let redisConf = {
      host: _.get(redisServer, 'host', 'localhost'),
      port,
      db: _.get(redisConfig, 'db', 3),
      retryStrategy: (times) => {
        const delay = Math.min(times * 1000, 300000)
        return delay
      }
    }

    acapi.aclog.serverInfo(redisConf)

    const opts = {
      createClient: (type) => {
        switch (type) {
          case 'client':
            return new Redis(redisConf)
          case 'subscriber':
            return new Redis(redisConf)
          default:
            return new Redis(redisConf)
        }
      }
    }

    // create a bull instance for every jobList, to allow concurrency
    _.forEach(_.get(params, 'jobLists'), jobList => {
      const { queueName } = this.prepareQueue(jobList)
      acapi.aclog.listing({ field: 'Queue', value: queueName })
      this.jobLists.push(queueName)

      acapi.bull[queueName] = new Queue(queueName, opts)
      if (_.get(params, 'activateListeners')) {
        if (_.get(jobList, 'listening')) {
          // this job's listener is on this API
          acapi.bull[queueName].on('global:completed', _.get(params, 'handlers.global:completed')[_.get(jobList, 'jobList')])
          acapi.bull[queueName].on('global:failed', _.get(params, 'handlers.global:failed', this.handleFailedJobs).bind(this, queueName))  
          acapi.aclog.listing({ field: '', value: 'Listener activated' })
        }
        if (_.get(jobList, 'worker')) {
          // this job's worker is on this API (BatchProcessCollector[jobList])
          let workerFN = _.get(params, 'worker')[_.get(jobList, 'jobList')]
          workerFN(jobList)
          acapi.aclog.listing({ field: '', value: 'Worker activated' })
        }
        if (_.get(jobList, 'autoClean')) {
          acapi.bull[queueName].clean(_.get(jobList, 'autoClean', _.get(acapi.config, 'bull.autoClean')))
        }
      }
    })
    return cb()
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

  const addJob = function(jobList, params, cb) {
    const functionIdentifier = _.padEnd('addJob', _.get(acapi.config, 'bull.log.functionIdentifierLength'))
    const { queueName } = this.prepareQueue({ jobList, configPath: _.get(params, 'configPath'), customJobList: _.get(params, 'customJobList') })
    if (!queueName) return cb({ message: 'jobListNotDefined', additionalInfo: { jobList } })

    const name = _.get(params, 'name') // named job
    const jobPayload = _.get(params, 'jobPayload')
    const jobOptions = _.get(params, 'jobOptions', {})
    if (_.get(jobPayload, 'jobId')) {
      _.set(jobOptions, 'jobId', _.get(jobPayload, 'jobId'))
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
    
    if (!acapi.bull[queueName]) return cb({ message: 'bullNotAvailableForQueueName', additionalInfo: { queueName } })
    //acapi.log.error('195 %j %j %j %j %j', queueName, name, jobPayload, jobOptions, addToWatchList)

    let jobId
    async.series({
      addJob: (done) => {
        if (name) {
          acapi.bull[queueName].add(name, jobPayload, jobOptions).then(job => {
            jobId = _.get(job, 'id')
            return done()
          }).catch(err => {
            acapi.log.error('%s | %s | %s | Name %s | Adding job failed %j', functionName, functionIdentifier, queueName, name, err)
          })
        }
        else {
          acapi.bull[queueName].add(jobPayload, jobOptions).then(job => {
            jobId = _.get(job, 'id')
            return done()
          }).catch(err => {
            acapi.log.error('%s | %s | %s | Adding job failed %j', functionName, functionIdentifier, queueName, err)
          })
        }
      },
      addKeyToWatchList: (done) => {
        if (!addToWatchList || !jobListWatchKey || !_.isObject(acapi.redis[_.get(acapi.config, 'bull.redis.database.name')])) return done()
        acapi.redis[_.get(acapi.config, 'bull.redis.database.name')].hset(jobListWatchKey, jobId, queueName, done)
      }
    }, (err) => {
      if (_.get(params, 'debug')) acapi.log.info('%s | %s | %s | %s | Adding job to queue', functionName, functionIdentifier, queueName, jobId)
      return cb(err, { jobId })
    })
  }

  const removeJob = (job, queueName) => {
    const functionIdentifier = _.padEnd('removeJob', _.get(acapi.config, 'bull.log.functionIdentifierLength'))
    if (_.isNil(job)) {
      acapi.log.error('%s | %s | %s | Job invalid %j', functionName, functionIdentifier, queueName, job)
    }
    const jobId = _.get(job, 'id')
    const jobListWatchKey = _.get(job, 'data.jobListWatchKey')

    async.series({
      removeKeyFromWatchList: (done) => {
        if (!jobListWatchKey || !_.isObject(acapi.redis[_.get(acapi.config, 'bull.redis.database.name')])) return done()        
        acapi.redis[_.get(acapi.config, 'bull.redis.database.name')].hdel(jobListWatchKey, jobId, done)
      },
      removeJob: (done) => {
        job.remove()
        return done()
      }
    }, function allDone(err) {
      if (err) acapi.log.error('%s | %s | %s | %s | Failed %j', functionName, functionIdentifier, queueName, jobId, err)
      else acapi.log.info('%s | %s | %s | # %s | Successful', functionName, functionIdentifier, queueName, jobId)
    })
  }

  const prepareProcessing = function(params, cb) {
    const functionIdentifier = _.padEnd('prepareProcessing', _.get(acapi.config, 'bull.log.functionIdentifierLength'))
    const jobList = _.get(params, 'jobList') 
    const jobId = _.get(params, 'jobId')
    const that = this

    const redisKey = acapi.config.environment + ':bull:' + _.get(jobList, 'jobList') + ':' + jobId + ':complete:lock'
    const { queueName, jobListConfig } = this.prepareQueue({ jobList, configPath: _.get(params, 'configPath') })
    if (!queueName) return cb({ message: 'queueNameMissing', additionalInfo: params })
    const retentionTime = _.get(jobListConfig, 'retentionTime', _.get(acapi.config, 'bull.retentionTime', 60000))
    
    redisLock.lockKey({ redisKey }, err => {
      if (err === 423) {
        acapi.log.debug('%s | %s | %s | # %s | Already processing', functionName, functionIdentifier, queueName, jobId)
        if (_.isFunction(cb)) return cb(423)
      }
      if (err) {
        acapi.log.error('%s | %s | %s | # %s | Failed %j', functionName, functionIdentifier, queueName, jobId, err)
        if (_.isFunction(cb)) return cb(err)     
      }
      acapi.bull[queueName].getJob(jobId).then((result) => {
        acapi.log.info('%s | %s | %s | # %s | C/MC %s/%s', functionName, functionIdentifier, queueName, jobId, _.get(result, 'data.customerId', '-'), _.get(result, 'data.mediaContainerId', '-'))
        setTimeout(that.removeJob, retentionTime, result, queueName)
        if (_.isFunction(cb)) return cb(null, result)
        acapi.log.info('%s | %s | %s | # %s | Successful', functionName, functionIdentifier, queueName, jobId, _.get(result, 'data.customerId', '-'), _.get(result, 'data.mediaContainerId', '-'))
      }).catch(err => {
        acapi.log.error('%s | %s | %s | # %s | Failed %j', functionName, functionIdentifier, queueName, jobId, err)
        if (_.isFunction(cb)) return cb()
      })
    })
  }

  /**
   * Shutdown all queues/redis connections
   */
  const shutdown = function(cb) {
    async.each(this.jobLists, (queueName, itDone) => {
      acapi.bull[queueName].close().then(itDone)
    }, cb)
  }

  return {
    init,
    scope,
    jobLists,
    prepareQueue,
    handleFailedJobs,
    prepareProcessing,
    addJob,
    removeJob,
    shutdown
  }

}