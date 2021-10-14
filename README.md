# AdmiralCloud Bull Queue List
This helper initialitzes Bull lists and helper functions and makes them available on a globally scoped variable.

# Usage
```
const bullOptions = {
  activateListeners: true/false,
  worker: BatchProcessCollector, // optional module that has all worker functions available
  handlers: {
    'global:completed': BullHelper // optional module that reacts to global:completed events - functions in there must be named after the jobList name (e.g. sendEmail)
  },
  jobLists: [{
    jobList: 'sendEmail',
    enabled: true
  }, ...]
}

// acapi should be globally available throughout your app to access Bull functions
const acapi = {}
// make bull (and bull helper functions) available on acapi.bull
acapi.bull = acbBull(acapi)
// init the bull lists
acapi.bull.init(bullOptions, done)
```

TBC