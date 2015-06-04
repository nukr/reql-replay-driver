import rethinkdbdash from 'rethinkdbdash'

const r = rethinkdbdash({
  host: '127.0.0.1',
  port: 30000
})

let term = {}

class DB {
  constructor (dbName) {
    this.dbName = dbName
    return r.db(this.dbName)
  }
}

class TABLE {
  constructor (tableName, pre) {
    this.tableName = tableName
    return pre.table(tableName)
  }
}

class FILTER {
  constructor (filter, pre) {
    this.filter = filter
    return pre.filter(filter)
  }
}

class FUNC {
  constructor () {
  }

  func (...args) {
  }
}

term.DB = DB
term.TABLE = TABLE
term.FILTER = FILTER

export default term
