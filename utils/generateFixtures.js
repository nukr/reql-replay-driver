import rethinkdbdash from 'rethinkdbdash'
import co from 'co'
import config from '../config'
console.log(config)

const r = rethinkdbdash({host: config.rethinkdb.host, port: config.rethinkdb.port})

co(function * () {
  console.log('generating fixtures ...')
  console.time('generateFixutures')
  let dbList = yield r.dbList().run()
  if (dbList.indexOf(config.rethinkdb.db) === -1) {
    yield r.dbCreate(config.rethinkdb.db)
  } else {
    yield r.dbDrop(config.rethinkdb.db)
    yield r.dbCreate(config.rethinkdb.db)
  }

  let tableList = yield r.db(config.rethinkdb.db).tableList()
  if (tableList.indexOf('sequence') === -1) {
    yield r.db(config.rethinkdb.db).tableCreate('sequence')
  } else {
    yield r.db(config.rethinkdb.db).tableDrop('sequence')
    yield r.db(config.rethinkdb.db).tableCreate('sequence')
  }

  if (tableList.indexOf('insert') === -1) {
    yield r.db(config.rethinkdb.db).tableCreate('insert')
  } else {
    yield r.db(config.rethinkdb.db).tableDrop('insert')
    yield r.db(config.rethinkdb.db).tableCreate('insert')
  }

  let promises = Array.from(Array(100)).map((e, index) => {
    let name = ['sunny', 'noel', 'wei', 'victor', 'tc', 'stan', 'annie', 'tyler']
    return r.db(config.rethinkdb.db).table('sequence').insert({
      name: name[Math.floor(Math.random() * 8)],
      num: index
    }).run()
  })

  let result = yield Promise.all(promises)

  yield r.db(config.rethinkdb.db).table('sequence').indexCreate('name')
  console.time('indexWait name')
  yield r.db(config.rethinkdb.db).table('sequence').indexWait('name')
  console.timeEnd('indexWait name')

  yield r.db(config.rethinkdb.db).table('sequence').indexCreate('num')
  console.time('indexWait num')
  yield r.db(config.rethinkdb.db).table('sequence').indexWait('num')
  console.timeEnd('indexWait num')

  r.getPoolMaster().drain()
  console.timeEnd('generateFixutures')
})

