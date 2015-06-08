import rethinkdbdash from 'rethinkdbdash'
import co from 'co'
import config from '../../config.js'
console.log(config)

const r = rethinkdbdash(config.rethinkdb)

co(function * () {
  console.log('generating fixtures ...')
  console.time('generateFixutures')
  let dbList = yield r.dbList().run()
  if (dbList.indexOf('driverTest') === -1) {
    yield r.dbCreate('driverTest')
  } else {
    yield r.dbDrop('driverTest')
    yield r.dbCreate('driverTest')
  }

  let tableList = yield r.db('driverTest').tableList()
  if (tableList.indexOf('sequence') === -1) {
    yield r.db('driverTest').tableCreate('sequence')
  } else {
    yield r.db('driverTest').tableDrop('sequence')
    yield r.db('driverTest').tableCreate('sequence')
  }

  if (tableList.indexOf('insert') === -1) {
    yield r.db('driverTest').tableCreate('insert')
  } else {
    yield r.db('driverTest').tableDrop('insert')
    yield r.db('driverTest').tableCreate('insert')
  }

  let promises = Array.from(Array(100)).map((e, index) => {
    let name = ['sunny', 'noel', 'wei', 'victor', 'tc', 'stan', 'annie', 'tyler']
    return r.db('driverTest').table('sequence').insert({
      name: name[Math.floor(Math.random() * 8)],
      num: index
    }).run()
  })

  let result = yield Promise.all(promises)
  r.getPoolMaster().drain()
  console.timeEnd('generateFixutures')
})

