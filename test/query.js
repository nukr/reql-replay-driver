/* global describe, it */
import chai, {expect} from 'chai'
import chaiAsPromised from 'chai-as-promised'
import Query from '../src/query'
import r from 'rethinkdb'
import co from 'co'
import config from '../config'
import Debug from 'debug'

chai.use(chaiAsPromised)

let debug = Debug('reql:test')
let seqTable = r.db(config.rethinkdb.db).table('sequence')

before(() => {
  Query.options(config.rethinkdb)
})

describe('writing data', () => {
  it('insert', (done) => {
    let insertQuery = new Query(r.db(config.rethinkdb.db).table('insert').insert({test: 'gg'}).build(), config.rethinkdb.db)
    co(function * () {
      let insertResult = yield insertQuery.run()
      expect(insertResult).to.include.keys('inserted')
      expect(insertResult.inserted).to.be.equal(1)
      done()
    }).catch(done)
  })
  it('update', (done) => {
    let updateQuery = new Query(r.db(config.rethinkdb.db).table('insert').nth(0).update({test: 'qq'}).build(), config.rethinkdb.db)
    co(function * () {
      let updateResult = yield updateQuery.run()
      expect(updateResult).to.include.keys('inserted')
      expect(updateResult.replaced).to.be.equal(1)
      done()
    }).catch(done)
  })
  it('replace', (done) => {
    done()
  })
  it('delete', (done) => {
    let deleteQuery = new Query(r.db(config.rethinkdb.db).table('insert').delete().build(), config.rethinkdb.db)
    co(function * () {
      let deleteResult = yield deleteQuery.run()
      expect(deleteResult).to.include.keys('deleted')
      expect(deleteResult.deleted).to.be.equal(1)
      done()
    }).catch(done)
  })
})

describe('selecting data', () => {
  it('get', (done) => {
    co(function * () {
      let firstQuery = new Query(r.db(config.rethinkdb.db).table('sequence').filter({num: 1}).nth(0).build(), config.rethinkdb.db)
      let firstResult = yield firstQuery.run()
      let getQuery = new Query(r.db(config.rethinkdb.db).table('sequence').get(firstResult.id).build(), config.rethinkdb.db)
      let getResult = yield getQuery.run()
      expect(getResult.num).to.be.equal(1)
      done()
    }).catch(done)
  })
  it('getAll', (done) => {
    co(function * () {
      let getAllQuery = new Query(r.db(config.rethinkdb.db).table('sequence').getAll('stan', 'qq', {index: 'name'}).build(), config.rethinkdb.db)
      let getAllResult = yield getAllQuery.run()
      done()
    }).catch(done)
  })
  it('between primary key', (done) => {
    co(function * () {
      let betweenQuery = new Query(r.db(config.rethinkdb.db).table('sequence').between(10, 20).build(), config.rethinkdb.db)
      let betweenResult = yield betweenQuery.run()
      done()
    }).catch(done)
  })
  it('between specify key', (done) => {
    co(function * () {
      let betweenQuery = new Query(r.db(config.rethinkdb.db).table('sequence').between(10, 20, {index: 'name'}).build(), config.rethinkdb.db)
      let betweenResult = yield betweenQuery.run()
      done()
    }).catch(done)
  })
  it('filter', (done) => {
    co(function * () {
      let filterQuery = new Query(r.db(config.rethinkdb.db).table('sequence').filter({num: 1}).nth(0).build(), config.rethinkdb.db)
      let filterResult = yield filterQuery.run()
      expect(filterResult.num).to.be.equal(1)
      done()
    }).catch(done)
  })
})

describe('joins', () => {
  it('innerJoin', (done) => {
    let query = new Query(r.db(config.rethinkdb.db).table('sequence').innerJoin(r.db(config.rethinkdb.db).table('test')).build())
    let fn = () => {
      query.run()
    }
    expect(fn).to.throw(Error)
    done()
  })
  it('outerJoin', (done) => {
    let query = new Query(r.db(config.rethinkdb.db).table('sequence').outerJoin(r.db(config.rethinkdb.db).table('test')).build())
    let fn = () => {
      query.run()
    }
    expect(fn).to.throw(Error)
    done()
  })
  it('eqJoin', (done) => {
    let query = new Query(r.db(config.rethinkdb.db).table('sequence').eqJoin('id', r.db(config.rethinkdb.db).table('sequence')).build())
    let fn = () => {
      query.run()
    }
    expect(fn).to.throw(Error)
    done()
  })
  it('zip', (done) => {
    co(function * () {
      done()
    }).catch(done)
  })
})


describe('math and logic', () => {
  it('add', (done) => {
    let add = new Query(r.expr(12).add(2).build(), config.rethinkdb.db)
    co(function * () {
      try {
        let result = yield add.run()
        expect(result).to.be.equal(14)
        done()
      } catch (e) {
        done(e)
      }
    })
  })
  it('sub', (done) => {
    let sub = new Query(r.expr(12).sub(2).build(), config.rethinkdb.db)
    co(function * () {
      try {
        let result = yield sub.run()
        expect(result).to.be.equal(10)
        done()
      } catch (e) {
        done(e)
      }
    })
  })
  it('mul', (done) => {
    let mul = new Query(r.expr(12).mul(2).build(), config.rethinkdb.db)
    co(function * () {
      try {
        let result = yield mul.run()
        expect(result).to.be.equal(24)
        done()
      } catch (e) {
        done(e)
      }
    })
  })
  it('div', (done) => {
    let div = new Query(r.expr(12).div(2).build(), config.rethinkdb.db)
    co(function * () {
      try {
        let result = yield div.run()
        expect(result).to.be.equal(6)
        done()
      } catch (e) {
        done(e)
      }
    })
  })

  it('and', (done) => {
    let and = new Query(r.expr(true).and(true).build(), config.rethinkdb.db)
    co(function * () {
      try {
        let result = yield and.run()
        expect(result).to.be.true
        done()
      } catch (e) {
        done(e)
      }
    })
  })

  it('or', (done) => {
    let or = new Query(r.expr(true).or(true).build(), config.rethinkdb.db)
    co(function * () {
      try {
        let result = yield or.run()
        expect(result).to.be.true
        done()
      } catch (e) {
        done(e)
      }
    })
  })

  it('eq', (done) => {
    let eq = new Query(r.expr(true).eq(true).build(), config.rethinkdb.db)
    co(function * () {
      try {
        let result = yield eq.run()
        expect(result).to.be.true
        done()
      } catch (e) {
        done(e)
      }
    })
  })

  it('ne', (done) => {
    let ne = new Query(r.expr(true).ne(false).build(), config.rethinkdb.db)
    co(function * () {
      try {
        let result = yield ne.run()
        expect(result).to.be.true
        done()
      } catch (e) {
        done(e)
      }
    })
  })

  it('ge', (done) => {
    let ge1 = new Query(r.expr(12).ge(12).build(), config.rethinkdb.db)
    let ge2 = new Query(r.expr(12).ge(14).build(), config.rethinkdb.db)
    let ge3 = new Query(r.expr(12).ge(10).build(), config.rethinkdb.db)
    co(function * () {
      try {
        let ge1Result = yield ge1.run()
        expect(ge1Result).to.be.true
        let ge2Result = yield ge2.run()
        expect(ge2Result).to.be.false
        let ge3Result = yield ge3.run()
        expect(ge3Result).to.be.true
        done()
      } catch (e) {
        done(e)
      }
    })
  })

  it('le', (done) => {
    let le1 = new Query(r.expr(12).le(12).build(), config.rethinkdb.db)
    let le2 = new Query(r.expr(12).le(14).build(), config.rethinkdb.db)
    let le3 = new Query(r.expr(12).le(10).build(), config.rethinkdb.db)
    co(function * () {
      try {
        let le1Result = yield le1.run()
        expect(le1Result).to.be.true
        let le2Result = yield le2.run()
        expect(le2Result).to.be.true
        let le3Result = yield le3.run()
        expect(le3Result).to.be.false
        done()
      } catch (e) {
        done(e)
      }
    })
  })

  it('not', (done) => {
    let not1 = new Query(r.expr(true).not().build(), config.rethinkdb.db)
    let not2 = new Query(r.not(false).build(), config.rethinkdb.db)
    co(function * () {
      try {
        let not1Result = yield not1.run()
        expect(not1Result).to.be.false
        let not2Result = yield not2.run()
        expect(not2Result).to.be.true
        done()
      } catch (e) {
        done(e)
      }
    })
  })

  it('random', (done) => {
    let random1 = new Query(r.random().build(), config.rethinkdb.db)
    let random2 = new Query(r.random(100, 200, {float: true}).build(), config.rethinkdb.db)
    let random3 = new Query(r.random(100, 200).build(), config.rethinkdb.db)
    co(function * () {
      try {
        let random1Result = yield random1.run()
        expect(random1Result).to.be.a('number')
        let random2Result = yield random2.run()
        expect(random2Result).to.be.a('number')
        let random3Result = yield random3.run()
        expect(Number.isInteger(random3Result)).to.be.true
        done()
      } catch (e) {
        done(e)
      }
    })
  })
})

describe('date and times', () => {
  it('now', (done) => {
    let now = new Query(r.now().build(), config.rethinkdb.db)
    co(function * () {
      try {
        let result = yield now.run()
        expect(result).to.be.instanceof(Date)
        done()
      } catch (e) {
        done(e)
      }
    })
  })

  it('time', (done) => {
    let time = new Query(r.time(1988, 4, 5, 12, 12, 12, 'Z').build(), config.rethinkdb.db)
    co(function * () {
      try {
        let result = yield time.run()
        expect(result).to.be.instanceof(Date)
        done()
      } catch (e) {
        done(e)
      }
    })
  })

  it('epochTime', (done) => {
    let epochTime = new Query(r.epochTime(531360000).build(), config.rethinkdb.db)
    co(function * () {
      try {
        let result = yield epochTime.run()
        expect(result).to.be.instanceof(Date)
        done()
      } catch (e) {
        done(e)
      }
    })
  })

  it('ISO8601', (done) => {
    let ISO8601 = new Query(r.ISO8601('1986-11-03T08:30:00-07:00').build(), config.rethinkdb.db)
    co(function * () {
      try {
        let result = yield ISO8601.run()
        expect(result).to.be.instanceof(Date)
        done()
      } catch (e) {
        done(e)
      }
    })
  })

  it('inTimezone', (done) => {
    let inTimezone = new Query(r.now().inTimezone('+08:00').hours().build(), config.rethinkdb.db)
    co(function * () {
      try {
        let result = yield inTimezone.run()
        expect(result).to.be.equal(new Date().getHours())
        done()
      } catch (e) {
        done(e)
      }
    })
  })

  it('timezone', (done) => {
    let timezone = new Query(r.now().timezone().build(), config.rethinkdb.db)
    co(function * () {
      try {
        let result = yield timezone.run()
        expect(result).to.be.equal('+00:00')
        done()
      } catch (e) {
        done(e)
      }
    })
  })

  it('during', (done) => {
    let during = new Query(r.time(2015, 5, 4, 'Z').during(r.time(2015, 4, 4, 'Z'), r.time(2015, 5, 6, 'Z')).build(), config.rethinkdb.db)
    co(function * () {
      try {
        let result = yield during.run()
        expect(result).to.be.true
        done()
      } catch (e) {
        done(e)
      }
    })
  })

  it('date', (done) => {
    let date = new Query(r.now().date().hours().build(), config.rethinkdb.db)
    co(function * () {
      try {
        let result = yield date.run()
        expect(result).to.be.equal(0)
        done()
      } catch (e) {
        done(e)
      }
    })
  })

  it('timeOfDay', (done) => {
    let timeOfDay = new Query(r.time(2015, 4, 4, 'Z').timeOfDay().build(), config.rethinkdb.db)
    co(function * () {
      try {
        let result = yield timeOfDay.run()
        expect(result).to.be.a('number')
        done()
      } catch (e) {
        done(e)
      }
    })
  })

  it('year', (done) => {
    let year = new Query(r.time(2015, 4, 4, 'Z').year().build(), config.rethinkdb.db)
    co(function * () {
      try {
        let result = yield year.run()
        expect(result).to.be.equal(2015)
        done()
      } catch (e) {
        done(e)
      }
    })
  })

  it('month', (done) => {
    let month = new Query(r.time(2015, 4, 4, 'Z').month().build(), config.rethinkdb.db)
    co(function * () {
      try {
        let result = yield month.run()
        expect(result).to.be.equal(4)
        done()
      } catch (e) {
        done(e)
      }
    })
  })

  it('day', (done) => {
    let day = new Query(r.time(2015, 4, 4, 'Z').day().build(), config.rethinkdb.db)
    co(function * () {
      try {
        let result = yield day.run()
        expect(result).to.be.equal(4)
        done()
      } catch (e) {
        done(e)
      }
    })
  })

  it('dayOfWeek', (done) => {
    let dayOfWeek = new Query(r.time(2015, 4, 4, 'Z').dayOfWeek().build(), config.rethinkdb.db)
    co(function * () {
      try {
        let result = yield dayOfWeek.run()
        expect(result).to.be.equal(6)
        done()
      } catch (e) {
        done(e)
      }
    })
  })

  it('dayOfYear', (done) => {
    let dayOfYear = new Query(r.time(2015, 4, 4, 'Z').dayOfYear().build(), config.rethinkdb.db)
    co(function * () {
      try {
        let result = yield dayOfYear.run()
        expect(result).to.be.equal(94)
        done()
      } catch (e) {
        done(e)
      }
    })
  })

  it('hours', (done) => {
    let hours = new Query(r.time(2015, 5, 5, 12, 12, 12, 'Z').hours().build(), config.rethinkdb.db)
    co(function * () {
      try {
        let result = yield hours.run()
        expect(result).to.be.equal(12)
        done()
      } catch (e) {
        done(e)
      }
    })
  })

  it('minutes', (done) => {
    let minutes = new Query(r.time(2015, 5, 5, 12, 12, 12, 'Z').minutes().build(), config.rethinkdb.db)
    co(function * () {
      try {
        let result = yield minutes.run()
        expect(result).to.be.equal(12)
        done()
      } catch (e) {
        done(e)
      }
    })
  })

  it('seconds', (done) => {
    let seconds = new Query(r.time(2015, 5, 5, 12, 12, 12, 'Z').seconds().build(), config.rethinkdb.db)
    co(function * () {
      try {
        let result = yield seconds.run()
        expect(result).to.be.equal(12)
        done()
      } catch (e) {
        done(e)
      }
    })
  })

  it('toISO8601', (done) => {
    let toISO8601 = new Query(r.time(2015, 5, 5, 12, 12, 12, 'Z').toISO8601().build(), config.rethinkdb.db)
    co(function * () {
      try {
        let result = yield toISO8601.run()
        let conn = yield r.connect({host: config.rethinkdb.host, port: config.rethinkdb.port})
        let rResult = yield r.time(2015, 5, 5, 12, 12, 12, 'Z').toISO8601().run(conn)
        expect(result).to.be.equal(rResult)
        done()
      } catch (e) {
        done(e)
      }
    })
  })

  it('toEpochTime', (done) => {
    let toEpochTime = new Query(r.time(2015, 5, 5, 12, 12, 12, 'Z').toEpochTime().build(), config.rethinkdb.db)
    co(function * () {
      try {
        let result = yield toEpochTime.run()
        let conn = yield r.connect({host: config.rethinkdb.host, port: config.rethinkdb.port})
        let rResult = yield r.time(2015, 5, 5, 12, 12, 12, 'Z').toEpochTime().run(conn)
        expect(result).to.be.equal(rResult)
        done()
      } catch (e) {
        done(e)
      }
    })
  })
})

describe('transformation', () => {
  it('map', (done) => {
    let query = new Query(r([1, 2, 3, 4, 5]).map((el) => {
      return el.add(2)
    }).build(), config.rethinkdb.db)
    co(function * () {
      try {
        let result = yield query.run()
        expect(result).to.be.eql([3, 4, 5, 6, 7])
        done()
      } catch (e) {
        done(e)
      }
    })
  })

  it('map reduce add', (done) => {
    let query = new Query(
      r.db(config.rethinkdb.db).table('sequence')
        .map(function(row){
          return 1
        })
        .reduce(function (left, right){
          return left.add(right)
        }).build(), config.rethinkdb.db
    )
    co(function * () {
        let result = yield query.run()
        expect(result).to.be.equal(100)
        done()
    }).catch(done)
  })

  it('withFields', (done) => {
    let query = new Query(r.db(config.rethinkdb.db).table('sequence').withFields('num').nth(0).build(), config.rethinkdb.db)
    co(function * () {
        let result = yield query.run()
        expect(Object.keys(result)).to.be.eql(['num'])
        done()
    }).catch(done)
  })

  it('concatMap', (done) => {
    let query = new Query(r([[1, 2, 3, 4, 5], [2, 3, 4, 5, 6], [3, 4, 5, 6, 7]]).concatMap((el) => {
      return el
    }).build(), config.rethinkdb.db)
    co(function * () {
      let result = yield query.run()
      expect(result).to.be.eql([1, 2, 3, 4, 5, 2, 3, 4, 5, 6, 3, 4, 5, 6, 7])
      done()
    }).catch(done)
  })

  // orderBy

  it('skip', (done) => {
    let query = new Query(r.db(config.rethinkdb.db).table('sequence').orderBy('num').skip(50).nth(0).build(), config.rethinkdb.db)
    co(function * () {
      let result = yield query.run()
      expect(result.num).to.be.equal(50)
      done()
    }).catch(done)
  })

  // limit
  it('limit', (done) => {
    let query = new Query(r.db(config.rethinkdb.db).table('sequence').limit(1).build(), config.rethinkdb.db)
    co(function * () {
      let result = yield query.run()
      expect(result.length).to.be.equal(1)
      done()
    }).catch(done)
  })

  // slice
  it('slice', (done) => {
    let arr100 = Array.apply(null, Array(100)).map((e, index) => index)
    let query = new Query(r(arr100).slice(0, 20).build(), config.rethinkdb.db)
    co(function * () {
      let result = yield query.run()
      expect(result.length).to.be.equal(20)
      done()
    }).catch(done)
  })

  // nth
  it('nth', (done) => {
    let arr100 = Array.apply(null, Array(100)).map((e, index) => index)
    let query = new Query(r(arr100).nth(20).build(), config.rethinkdb.db)
    co(function * () {
      let result = yield query.run()
      expect(result).to.be.equal(20)
      done()
    }).catch(done)
  })

  // offsetsOf
  it('offsetsOf', (done) => {
    let query = new Query(r(['a', 'b', 'c']).offsetsOf('c').nth(0).build(), config.rethinkdb.db)
    co(function * () {
      let result = yield query.run()
      expect(result).to.be.equal(2)
      done()
    }).catch(done)
  })

  // isEmpty
  it('isEmpty', (done) => {
    let query = new Query(r.db(config.rethinkdb.db).table('sequence').isEmpty().build(), config.rethinkdb.db)
    co(function * () {
      let result = yield query.run()
      expect(result).to.be.false
      done()
    }).catch(done)
  })

  // union
  // it('isEmpty', (done) => {
  //   let query = new Query(r.db('test').table('bills').isEmpty().build(), config.rethinkdb.db)
  //   co(function * () {
  //     let result = yield query.run()
  //     expect(result).to.be.false
  //     done()
  //   }).catch(done)
  // })
  // sample
  it('sample', (done) => {
    let query = new Query(r.db(config.rethinkdb.db).table('sequence').sample(5).build(), config.rethinkdb.db)
    co(function * () {
      let result = yield query.run()
      expect(result.length).to.be.equal(5)
      expect(result[0].num).to.be.a('number')
      done()
    }).catch(done)
  })
})

describe('aggregation', () => {
  it('group', (done) => {
    let query = new Query(r.db(config.rethinkdb.db).table('sequence').group('name').build(), config.rethinkdb.db)
    let groupInfoQuery = new Query(r.db(config.rethinkdb.db).table('sequence').group('name').info().build(), config.rethinkdb.db)
    co(function * () {
      let result = yield query.run()
      expect(result[0].group).to.be.exist
      expect(result[0].reduction).to.be.an('array')
      let groupInfo = yield groupInfoQuery.run()
      expect(groupInfo.type).to.be.equal('GROUPED_STREAM')
      done()
    }).catch(done)
  })

  it('ungroup', (done) => {
    let ungroupQuery = new Query(r.db(config.rethinkdb.db).table('sequence').group('name').ungroup().info().build(), config.rethinkdb.db)
    co(function * () {
      let ungroup = yield ungroupQuery.run()
      expect(ungroup.type).to.be.equal('ARRAY')
      done()
    }).catch(done)
  })

  it('reduce', (done) => {
    let reduceQuery = new Query(r.db(config.rethinkdb.db).table('sequence').map((seq) => {
      return 1
    }).reduce((left, right) => {
      return left.add(right)
    }).build(), config.rethinkdb.db)
    co(function * () {
      let reduce = yield reduceQuery.run()
      expect(reduce).to.be.equal(100)
      done()
    }).catch(done)
  })

  it('count', (done) => {
    let countQuery = new Query(r.db(config.rethinkdb.db).table('sequence').count().build(), config.rethinkdb.db)
    co(function * () {
      let count = yield countQuery.run()
      expect(count).to.be.equal(100)
      done()
    }).catch(done)
  })

  it('sum', (done) => {
    let sumQuery = new Query(r.expr([3, 5, 7]).sum().build(), config.rethinkdb.db)
    co(function * () {
      let sum = yield sumQuery.run()
      expect(sum).to.be.equal(15)
      done()
    }).catch(done)
  })

  it('avg', (done) => {
    let avgQuery = new Query(r.expr([3, 5, 7]).avg().build(), config.rethinkdb.db)
    co(function * () {
      let avg = yield avgQuery.run()
      expect(avg).to.be.equal(5)
      done()
    }).catch(done)
  })

  it('min', (done) => {
    let minQuery = new Query(r.expr([3, 5, 7]).min().build(), config.rethinkdb.db)
    co(function * () {
      let min = yield minQuery.run()
      expect(min).to.be.equal(3)
      done()
    }).catch(done)
  })

  it('max', (done) => {
    let maxQuery = new Query(r.expr([3, 5, 7]).max().build(), config.rethinkdb.db)
    co(function * () {
      let max = yield maxQuery.run()
      expect(max).to.be.equal(7)
      done()
    }).catch(done)
  })

  it('distinct', (done) => {
    let distinctQuery = new Query(r.expr([{name: 'wei'}, {name: 'sunny'}, {name: 'wei'}]).distinct().count().build(), config.rethinkdb.db)
    co(function * () {
      let distinct = yield distinctQuery.run()
      expect(distinct).to.be.equal(2)
      done()
    }).catch(done)
  })

  it('contains', (done) => {
    let containsQuery = new Query(r.expr([2, 3, 4, 5]).contains(3).build(), config.rethinkdb.db)
    co(function * () {
      let contains = yield containsQuery.run()
      expect(contains).to.be.true
      done()
    }).catch(done)
  })
})

describe('document manipulation', () => {
  it('row', (done) => {
    let rowQuery = new Query(r.db(config.rethinkdb.db).table('sequence').filter(r.row('num').ge(50)).count().build(), config.rethinkdb.db)
    co(function * () {
      let row = yield rowQuery.run()
      expect(row).to.be.equal(50)
      done()
    }).catch(done)
  })

  it('pluck', (done) => {
    let pluckQuery = new Query(seqTable.pluck('num', 'name').nth(0).build(), config.rethinkdb.db)
    co(function * () {
      let pluckResult = yield pluckQuery.run()
      expect(pluckResult).to.have.all.keys('name', 'num')
      done()
    }).catch(done)
  })

  it('without', (done) => {
    let withoutQuery = new Query(seqTable.without('num').nth(0).build(), config.rethinkdb.db)
    co(function * () {
      let withoutResult = yield withoutQuery.run()
      expect(withoutResult).to.have.all.keys('name', 'id')
      done()
    }).catch(done)
  })

  it('merge', (done) => {
    let mergeQuery = new Query(seqTable.filter({num: 1}).nth(0).merge({test: '123'}).build(), config.rethinkdb.db)
    co(function * () {
      let mergeResult = yield mergeQuery.run()
      expect(mergeResult).to.be.all.keys('name', 'num', 'test', 'id')
      done()
    }).catch(done)
  })

  it('append', (done) => {
    let appendQuery = new Query(seqTable.filter({num: 1}).coerceTo('array').append('test').build(), config.rethinkdb.db)
    co(function * () {
      let appendResult = yield appendQuery.run()
      expect(appendResult[1]).to.be.equal('test')
      done()
    }).catch(done)
  })

  it('prepend', (done) => {
    let prependQuery = new Query(seqTable.filter({num: 1}).coerceTo('array').prepend('test').build(), config.rethinkdb.db)
    co(function * () {
      let prependResult = yield prependQuery.run()
      expect(prependResult[0]).to.be.equal('test')
      done()
    }).catch(done)
  })

  it('difference', (done) => {
    let differenceQuery = new Query(r.expr([1, 2, 3, 4]).difference([1]).build(), config.rethinkdb.db)
    co(function * () {
      let differenceResult = yield differenceQuery.run()
      expect(differenceResult).to.be.eql([2, 3, 4])
      done()
    }).catch(done)
  })

  it('setInsert', (done) => {
    let setInsertQuery1 = new Query(r.expr([1, 2, 3, 4]).setInsert(1).build(), config.rethinkdb.db)
    let setInsertQuery2 = new Query(r.expr([1, 2, 3, 4]).setInsert(5).build(), config.rethinkdb.db)
    co(function * () {
      let setInsertResult1 = yield setInsertQuery1.run()
      expect(setInsertResult1).to.be.eql([1, 2, 3, 4])
      let setInsertResult2 = yield setInsertQuery2.run()
      expect(setInsertResult2).to.be.eql([1, 2, 3, 4, 5])
      done()
    }).catch(done)
  })

  it('setUnion', (done) => {
    let setUnionQuery = new Query(r.expr([1, 2, 3, 4]).setUnion([1, 2, 3, 4, 5]).build(), config.rethinkdb.db)
    co(function * () {
      let setUnionResult = yield setUnionQuery.run()
      expect(setUnionResult).to.be.eql([1, 2, 3, 4, 5])
      done()
    }).catch(done)
  })

  it('setIntersection', (done) => {
    let setIntersectionQuery = new Query(r.expr([1, 2, 3, 4]).setIntersection([1, 5, 6, 7]).build(), config.rethinkdb.db)
    co(function * () {
      let setIntersectionResult = yield setIntersectionQuery.run()
      expect(setIntersectionResult).to.be.eql([1])
      done()
    }).catch(done)
  })

  it('setDifference', (done) => {
    let setDifferenceQuery = new Query(r.expr([1, 2, 3, 4]).setDifference([1, 2, 3]).build(), config.rethinkdb.db)
    co(function * () {
      let setDifferenceResult = yield setDifferenceQuery.run()
      expect(setDifferenceResult).to.be.eql([4])
      done()
    }).catch(done)
  })

  it('()', (done) => {
    co(function * () {
      done()
    }).catch(done)
  })

  it('getField', (done) => {
    let getFieldQuery = new Query(seqTable.getField('num').build(), config.rethinkdb.db)
    co(function * () {
      let getFieldResult = yield getFieldQuery.run()
      expect(getFieldResult).to.have.length(100)
      expect(getFieldResult).to.be.an('array')
      done()
    }).catch(done)
  })

  it('hasFields', (done) => {
    let hasFieldsQuery = new Query(seqTable.hasFields(['num']).build(), config.rethinkdb.db)
    co(function * () {
      let hasFieldsResult = yield hasFieldsQuery.run()
      expect(hasFieldsResult).to.have.length(100)
      done()
    }).catch(done)
  })

  it('insertAt', (done) => {
    let insertAtQuery = new Query(seqTable.coerceTo('array').insertAt(1, 'qq').build(), config.rethinkdb.db)
    co(function * () {
      let insertResult = yield insertAtQuery.run()
      expect(insertResult[1]).to.be.equal('qq')
      done()
    }).catch(done)
  })

  it('spliceAt', (done) => {
    let spliceAtQuery = new Query(r.expr([1, 2, 3, 4]).spliceAt(1, [5, 6]).build(), config.rethinkdb.db)
    co(function * () {
      let spliceResult = yield spliceAtQuery.run()
      expect(spliceResult).to.be.eql([1, 5, 6, 2, 3, 4])
      done()
    }).catch(done)
  })

  it('deleteAt', (done) => {
    let deleteAtQuery = new Query(r.expr([1, 2, 3, 4]).deleteAt(0).build(), config.rethinkdb.db)
    co(function * () {
      let deleteAtResult = yield deleteAtQuery.run()
      expect(deleteAtResult).to.be.eql([2, 3, 4])
      done()
    }).catch(done)
  })

  it('changeAt', (done) => {
    let changeAtQuery = new Query(r.expr([1, 2, 3, 4]).changeAt(0, 2).build(), config.rethinkdb.db)
    co(function * () {
      let changeAtResult = yield changeAtQuery.run()
      expect(changeAtResult).to.be.eql([2, 2, 3, 4])
      done()
    }).catch(done)
  })

  it('keys', (done) => {
    let keysQuery = new Query(seqTable.nth(0).keys().build(), config.rethinkdb.db)
    co(function * () {
      let keysResult = yield keysQuery.run()
      expect(keysResult).to.be.eql(['id', 'name', 'num'])
      done()
    }).catch(done)
  })

  // it('literal', (done) => {
  //   let literalQuery = new Query(seqTable.sample(1).nth(0).update({data: r.literal({aa: 'bb'})}).build(), config.rethinkdb.db)
  //   co(function * () {
  //     let literalResult = yield literalQuery.run()
  //     done()
  //   }).catch(done)
  // })

  it('object', (done) => {
    let objectQuery = new Query(r.object('key1', 11111, 'key2', 22222).build(), config.rethinkdb.db)
    co(function * () {
      let objectResult = yield objectQuery.run()
      expect(objectResult).to.be.eql({key1: 11111, key2: 22222})
      done()
    }).catch(done)
  })
})


describe('control structures', () => {
  it('args', (done) => {
    co(function * () {
      let argsQuery = new Query(r.args([1, 2, 3, 4, 5]).build(), config.rethinkdb.db)
      let argsResult = yield argsQuery.run()
      expect(argsResult).to.be.eql([1, 2, 3, 4, 5])
      done()
    }).catch(done)
  })

  it('do', (done) => {
    co(function * () {
      let doQuery = new Query(r.db(config.rethinkdb.db).table('sequence').orderBy('num', {index: 'num'}).nth(0).do((seq) => {
        return seq('num').add(100)
      }).build(), config.rethinkdb.db)
      let doResult = yield doQuery.run()
      expect(doResult).to.be.equal(100)
      done()
    }).catch(done)
  })
  it('branch', (done) => {
    co(function * () {
      let branchQuery = new Query(r.db('reql_driver_test').table('sequence').map(
        r.branch(
          r.row('num').gt(50),
          r.row('name').add('bigger'),
          r.row('name').add('smaller')
        )
      ).build(), config.rethinkdb.db)
      let branchResult = yield branchQuery.run()
      done()
    }).catch(done)
  })
  it('forEach', (done) => {
    co(function * () {
      done()
    }).catch(done)
  })
  it('range', (done) => {
    co(function * () {
      let rangeQuery = new Query(r.range(4).build(), config.rethinkdb.db)
      let rangeResult = yield rangeQuery.run()
      expect(rangeResult).to.be.eql([0, 1, 2, 3])
      done()
    }).catch(done)
  })
  it('error', (done) => {
    co(function * () {
      done()
    }).catch(done)
  })
  it('default', (done) => {
    co(function * () {
      done()
    }).catch(done)
  })
  it('expr', (done) => {
    co(function * () {
      done()
    }).catch(done)
  })
  it('typeOf', (done) => {
    co(function * () {
      done()
    }).catch(done)
  })
  it('json', (done) => {
    co(function * () {
      done()
    }).catch(done)
  })
  it('toJsonString', (done) => {
    co(function * () {
      done()
    }).catch(done)
  })
  it('toJSON', (done) => {
    co(function * () {
      done()
    }).catch(done)
  })
  it('http', (done) => {
    co(function * () {
      done()
    }).catch(done)
  })
  it('uuid', (done) => {
    co(function * () {
      done()
    }).catch(done)
  })


  it('info', (done) => {
    let db = new Query(r.db(config.rethinkdb.db).info().build(), config.rethinkdb.db)
    let table = new Query(r.db(config.rethinkdb.db).table('sequence').info().build(), config.rethinkdb.db)
    co(function * () {
      let dbResult = yield db.run()
      expect(dbResult.type).to.be.equal('DB')
      expect(dbResult.name).to.be.equal(config.rethinkdb.db)
      let tableResult = yield table.run()
      expect(tableResult.type).to.be.equal('TABLE')
      expect(tableResult.name).to.be.equal('sequence')
      done()
    }).catch(done)
  })

  it('coerceTo', (done) => {
    let coerceToQuery = new Query(r.db(config.rethinkdb.db).table('sequence').coerceTo('array').info().build(), config.rethinkdb.db)
    co(function * () {
      let coerceToResult = yield coerceToQuery.run()
      expect(coerceToResult.type).to.be.equal('ARRAY')
      done()
    }).catch(done)
  })
})

describe('string manipulation', () => {
  it('match', (done) => {
    co(function * () {
      done()
    }).catch(done)
  })
  it('split', (done) => {
    co(function * () {
      done()
    }).catch(done)
  })
  it('upcase', (done) => {
    let upcaseQuery = new Query(r.expr('hello world').upcase().build(), config.rethinkdb.db)
    co(function * () {
      let upcaseResult = yield upcaseQuery.run()
      expect(upcaseResult).to.be.equal('HELLO WORLD')
      done()
    }).catch(done)
  })
  it('downcase', (done) => {
    let downcaseQuery = new Query(r.expr('HELLO WORLD').downcase().build(), config.rethinkdb.db)
    co(function * () {
      let downcaseResult = yield downcaseQuery.run()
      expect(downcaseResult).to.be.equal('hello world')
      done()
    }).catch(done)
  })
})

describe('misc', () => {
  it('func', (done) => {
    let query = new Query(r.db(config.rethinkdb.db).table('sequence').filter((row) => {
      return row('num').lt(50)
    }).count().build(), config.rethinkdb.db)
    expect(query.run()).to.eventually.equal(50).notify(done)
  })
})

describe('can not access function', () => {
  it('dbCreate', (done) => {
    let query = new Query(r.dbCreate('qq123123').build(), config.rethinkdb.db)
    let fn = () => {
      query.run()
    }
    expect(fn).to.throw(Error, 'illegal query DB_CREATE')
    done()
  })

  it('innerJoin', (done) => {
    let query = new Query(r.db('hihi').table('hihi').innerJoin(r.db('qq').table('qq')).build(), config.rethinkdb.db)
    let fn = () => {
      query.run()
    }
    expect(fn).to.throw(Error, 'illegal query INNER_JOIN')
    done()
  })
})
