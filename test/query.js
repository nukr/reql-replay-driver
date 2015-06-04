/* global describe, it */
import {expect} from 'chai'
import Query from '../src/query'
import r from 'rethinkdb'
import co from 'co'

describe('build query from array', () => {
  it("simple query r.db('test').table('bills')", () => {
    let query = new Query(r.db('test').table('bills').build())
    let promise = query.run()
    promise.then((result) => {
      expect(result.length).to.be.equal(35177)
    })
  })

  it("filter with objet", () => {
    let query = new Query(r.db('test').table('bills').filter({billNo: 13799}).build())
    let promise = query.run()
    promise.then((result) => {
      expect(result[0].id).to.be.equal('0006e572-2f24-4d79-87c3-ea6213caba06')
    })
  })

  it("filter with function", () => {
    let query = new Query(
      r.db('test').table('bills').filter((bill) => {
        return bill('id').eq('0006e572-2f24-4d79-87c3-ea6213caba06')
      }).build()
    )
    let promise = query.run()
    promise.then((result) => {
      expect(result[0].id).to.be.equal('0006e572-2f24-4d79-87c3-ea6213caba06')
    })
  })

  it("filter with function", () => {
    let query = new Query(
      r.db('test').table('bills').filter((bill) => {
        return bill('id').eq('0006e572-2f24-4d79-87c3-ea6213caba06')
      }).build()
    )
    let promise = query.run()
    promise.then((result) => {
      expect(result[0].id).to.be.equal('0006e572-2f24-4d79-87c3-ea6213caba06')
    })
  })

  it('map reduce add', (done) => {
    let query = new Query(
      r.db('test').table('bills')
        .map(function(bill){
          return 1
        })
        .reduce(function (left, right){
          return left.add(right)
        }).build()
    )
    co(function * () {
      try {
        let result = yield query.run()
        expect(result).to.be.equal(35177)
        done()
      } catch (e) {
        done(e)
      }
    })
  })

  it('withFields', (done) => {
    let query = new Query(r.db('test').table('bills').withFields('billNo', 'creator').limit(1).build())
    co(function * () {
      try {
        let result = yield query.run()
        expect(Object.keys(result[0])).to.be.eql(['billNo', 'creator'])
        done()
      } catch (e) {
        done(e)
      }
    })
  })

  it('count', (done) => {
    let query = new Query(r.db('test').table('bills').count().build())
    co(function * () {
      try {
        let result = yield query.run()
        expect(result).to.be.equal(35177)
        done()
      } catch (e) {
        done(e)
      }
    })
  })

  // it('expr', (done) => {
  //   let query = new Query(r.expr(12).build())
  //   co(function * () {
  //     try {
  //       let result = yield query.run()
  //       expect(result).to.be.equal(14)
  //       done()
  //     } catch (e) {
  //       done(e)
  //     }
  //   })
  // })
})

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

  // timezone
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

  // during
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

  // date
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

  // timeOfDay
  // year
  // month
  // day
  // dayOfWeek
  // dayOfYear

  // hours
  it('hours', (done) => {
    let hours = new Query(r.now().hours().build())
    co(function * () {
      try {
        let result = yield hours.run()
        expect(result).to.be.equal(new Date().getUTCHours())
        done()
      } catch (e) {
        done(e)
      }
    })
  })
  // minutes
  // seconds
  // toISO8601
  // toEpochTime
})
