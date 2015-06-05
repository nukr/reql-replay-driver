/* global describe, it */
import {expect} from 'chai'
import Query from '../src/query'
import r from 'rethinkdb'
import co from 'co'
import fs from 'fs'

// describe('selecting data', () => {
//   it('get')
//   it('getAll')
//   it('between')
//   it('filter')
// })

describe('arithmetics', () => {
  it('add', (done) => {
    let add = new Query(r.expr(12).add(2).build())
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
    let sub = new Query(r.expr(12).sub(2).build())
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
    let mul = new Query(r.expr(12).mul(2).build())
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
    let div = new Query(r.expr(12).div(2).build())
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
})

describe('logic', () => {
  it('and', (done) => {
    let and = new Query(r.expr(true).and(true).build())
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
    let or = new Query(r.expr(true).or(true).build())
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
    let eq = new Query(r.expr(true).eq(true).build())
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
    let ne = new Query(r.expr(true).ne(false).build())
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
    let ge1 = new Query(r.expr(12).ge(12).build())
    let ge2 = new Query(r.expr(12).ge(14).build())
    let ge3 = new Query(r.expr(12).ge(10).build())
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
    let le1 = new Query(r.expr(12).le(12).build())
    let le2 = new Query(r.expr(12).le(14).build())
    let le3 = new Query(r.expr(12).le(10).build())
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
    let not1 = new Query(r.expr(true).not().build())
    let not2 = new Query(r.not(false).build())
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
    let random1 = new Query(r.random().build())
    let random2 = new Query(r.random(100, 200, {float: true}).build())
    let random3 = new Query(r.random(100, 200).build())
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
    let now = new Query(r.now().build())
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
    let time = new Query(r.time(1988, 4, 5, 12, 12, 12, 'Z').build())
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
    let epochTime = new Query(r.epochTime(531360000).build())
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
    let ISO8601 = new Query(r.ISO8601('1986-11-03T08:30:00-07:00').build())
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
    let inTimezone = new Query(r.now().inTimezone('+08:00').hours().build())
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
    let timezone = new Query(r.now().timezone().build())
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
    let during = new Query(r.time(2015, 5, 4, 'Z').during(r.time(2015, 4, 4, 'Z'), r.time(2015, 5, 6, 'Z')).build())
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
    let date = new Query(r.now().date().hours().build())
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
    let timeOfDay = new Query(r.time(2015, 4, 4, 'Z').timeOfDay().build())
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
    let year = new Query(r.time(2015, 4, 4, 'Z').year().build())
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
    let month = new Query(r.time(2015, 4, 4, 'Z').month().build())
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
    let day = new Query(r.time(2015, 4, 4, 'Z').day().build())
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
    let dayOfWeek = new Query(r.time(2015, 4, 4, 'Z').dayOfWeek().build())
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
    let dayOfYear = new Query(r.time(2015, 4, 4, 'Z').dayOfYear().build())
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
    let hours = new Query(r.time(2015, 5, 5, 12, 12, 12, 'Z').hours().build())
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
    let minutes = new Query(r.time(2015, 5, 5, 12, 12, 12, 'Z').minutes().build())
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
    let seconds = new Query(r.time(2015, 5, 5, 12, 12, 12, 'Z').seconds().build())
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
    let toISO8601 = new Query(r.time(2015, 5, 5, 12, 12, 12, 'Z').toISO8601().build())
    co(function * () {
      try {
        let result = yield toISO8601.run()
        let conn = yield r.connect({host: '192.168.100.5', port: 28015})
        let rResult = yield r.time(2015, 5, 5, 12, 12, 12, 'Z').toISO8601().run(conn)
        expect(result).to.be.equal(rResult)
        done()
      } catch (e) {
        done(e)
      }
    })
  })

  it('toEpochTime', (done) => {
    let toEpochTime = new Query(r.time(2015, 5, 5, 12, 12, 12, 'Z').toEpochTime().build())
    co(function * () {
      try {
        let result = yield toEpochTime.run()
        let conn = yield r.connect({host: '192.168.100.5', port: 28015})
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
    }).build())
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
      r.db('driverTest').table('sequence')
        .map(function(row){
          return 1
        })
        .reduce(function (left, right){
          return left.add(right)
        }).build()
    )
    co(function * () {
        let result = yield query.run()
        expect(result).to.be.equal(100)
        done()
    }).catch(done)
  })

  it('withFields', (done) => {
    let query = new Query(r.db('driverTest').table('sequence').withFields('num').nth(0).build())
    co(function * () {
        let result = yield query.run()
        expect(Object.keys(result)).to.be.eql(['num'])
        done()
    }).catch(done)
  })

  it('concatMap', (done) => {
    let query = new Query(r([[1, 2, 3, 4, 5], [2, 3, 4, 5, 6], [3, 4, 5, 6, 7]]).concatMap((el) => {
      return el
    }).build())
    co(function * () {
      let result = yield query.run()
      expect(result).to.be.eql([1, 2, 3, 4, 5, 2, 3, 4, 5, 6, 3, 4, 5, 6, 7])
      done()
    }).catch(done)
  })

  // orderBy

  it('skip', (done) => {
    let query = new Query(r.db('driverTest').table('sequence').orderBy('num').skip(50).nth(0).build())
    co(function * () {
      let result = yield query.run()
      expect(result.num).to.be.equal(50)
      done()
    }).catch(done)
  })

  // limit
  it('limit', (done) => {
    let query = new Query(r.db('driverTest').table('sequence').limit(1).build())
    co(function * () {
      let result = yield query.run()
      expect(result.length).to.be.equal(1)
      done()
    }).catch(done)
  })

  // slice
  it('slice', (done) => {
    let arr100 = Array.apply(null, Array(100)).map((e, index) => index)
    let query = new Query(r(arr100).slice(0, 20).build())
    co(function * () {
      let result = yield query.run()
      expect(result.length).to.be.equal(20)
      done()
    }).catch(done)
  })

  // nth
  it('nth', (done) => {
    let arr100 = Array.apply(null, Array(100)).map((e, index) => index)
    let query = new Query(r(arr100).nth(20).build())
    co(function * () {
      let result = yield query.run()
      expect(result).to.be.equal(20)
      done()
    }).catch(done)
  })

  // offsetsOf
  it('offsetsOf', (done) => {
    let query = new Query(r(['a', 'b', 'c']).offsetsOf('c').nth(0).build())
    co(function * () {
      let result = yield query.run()
      expect(result).to.be.equal(2)
      done()
    }).catch(done)
  })

  // isEmpty
  it('isEmpty', (done) => {
    let query = new Query(r.db('driverTest').table('sequence').isEmpty().build())
    co(function * () {
      let result = yield query.run()
      expect(result).to.be.false
      done()
    }).catch(done)
  })

  // union
  // it('isEmpty', (done) => {
  //   let query = new Query(r.db('test').table('bills').isEmpty().build())
  //   co(function * () {
  //     let result = yield query.run()
  //     expect(result).to.be.false
  //     done()
  //   }).catch(done)
  // })
  // sample
  // it('isEmpty', (done) => {
  //   let query = new Query(r.db('test').table('bills').isEmpty().build())
  //   co(function * () {
  //     let result = yield query.run()
  //     expect(result).to.be.false
  //     done()
  //   }).catch(done)
  // })

})

describe('misc', () => {
  it('func', (done) => {
    let query = new Query(r.db('driverTest').table('sequence').filter((row) => {
      return row('num').lt(50)
    }).count().build())
    co(function * () {
      let result = yield query.run()
      expect(result).to.be.equal(50)
      done()
    }).catch(done)
  })
})
