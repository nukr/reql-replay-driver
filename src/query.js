import rethinkdbdash from 'rethinkdbdash'
import protodef from './protodef'
import debug from 'debug'
import config from '../config.js'

let log = debug('query')

let termTypes = protodef.Term.TermType
let r = rethinkdbdash(config.rethinkdb)

class Query {
  constructor (query) {
    this.query = query
  }

  run (query) {
    query = query || this.query
    let result = this.evaluate(query)
    log('result', result)
    return result.run()
  }

  evaluate (term, internalOptions) {
    internalOptions = internalOptions || {}

    if(!Array.isArray(term)) {
      return r.expr(term)
    }

    let termType = term[0]
    switch (termType) {
      case termTypes.DB: // 14
        return this.db(term[1])
      case termTypes.TABLE: // 15
        return this.table(term[1])
      case termTypes.FILTER: // 39
        return this.filter(term[1])
      case termTypes.FUNC: // 69
        return this.func(term[1])
      case termTypes.MAKE_ARRAY: // 2
        return this.makeArray(term[1])
      case termTypes.VAR: // 10
        return this.varId(term[1])
      case termTypes.BRACKET: // 170
        return this.bracket(term[1])
      case termTypes.REDUCE: // 37
        return this.reduce(term[1])
      case termTypes.MAP: // 38
        return this.map(term[1])
      case termTypes.ADD: // 24
        return this.add(term[1])
      case termTypes.SUB:
        return this.sub(term[1])
      case termTypes.MUL:
        return this.mul(term[1])
      case termTypes.DIV:
        return this.div(term[1])
      case termTypes.MOD:
        return this.mod(term[1])
      case termTypes.LIMIT: // 71
        return this.limit(term[1])
      case termTypes.WITH_FIELDS: // 96
        return this.withFields(term[1])
      case termTypes.COUNT:
        return this.count(term[1])
      case termTypes.AND:
        return this.and(term[1])
      case termTypes.OR:
        return this.or(term[1])
      case termTypes.EQ:
        return this.eq(term[1])
      case termTypes.NE:
        return this.ne(term[1])
      case termTypes.GT:
        return this.gt(term[1])
      case termTypes.GE:
        return this.ge(term[1])
      case termTypes.LT:
        return this.lt(term[1])
      case termTypes.LE:
        return this.le(term[1])
      case termTypes.NOT:
        return this.not(term[1])
      case termTypes.RANDOM:
        return this.random(term[1])
      case termTypes.NOW:
        return this.now(term[1])
      case termTypes.TIME:
        return this.time(term[1])
      case termTypes.EPOCH_TIME:
        return this.epochTime(term[1])
      case termTypes.ISO8601:
        return this.ISO8601(term[1])
      case termTypes.IN_TIMEZONE:
        return this.inTimezone(term[1])
      case termTypes.TIMEZONE:
        return this.timezone(term[1])
      case termTypes.DURING:
        return this.during(term[1])
      case termTypes.DATE:
        return this.date(term[1])
      case termTypes.TIME_OF_DAY:
        return this.timeOfDay(term[1])
      case termTypes.YEAR:
        return this.year(term[1])
      case termTypes.MONTH:
        return this.month(term[1])
      case termTypes.DAY:
        return this.day(term[1])
      case termTypes.DAY_OF_WEEK:
        return this.dayOfWeek(term[1])
      case termTypes.DAY_OF_YEAR:
        return this.dayOfYear(term[1])
      case termTypes.HOURS:
        return this.hours(term[1])
      case termTypes.MINUTES:
        return this.minutes(term[1])
      case termTypes.SECONDS:
        return this.seconds(term[1])
      case termTypes.TO_ISO8601:
        return this.toISO8601(term[1])
      case termTypes.TO_EPOCH_TIME:
        return this.toEpochTime(term[1])
      case termTypes.CONCAT_MAP:
        return this.concatMap(term[1])
      case termTypes.ORDER_BY:
        return this.orderBy(term[1])
      case termTypes.SKIP:
        return this.skip(term[1])
      case termTypes.SLICE:
        return this.slice(term[1])
      case termTypes.NTH:
        return this.nth(term[1])
      case termTypes.OFFSETS_OF:
        return this.offsetsOf(term[1])
      case termTypes.IS_EMPTY:
        return this.isEmpty(term[1])
      case termTypes.UNION:
        return this.union(term[1])
      case termTypes.SAMPLE:
        return this.sample(term[1])
      default:
        throw new Error.ReqlRuntimeError("Unknown term")
    }
  }

  db (args) {
    let dbName = args[0]
    log('db')
    return r.db(dbName)
  }

  table (args) {
    let db = this.evaluate(args[0])
    let tableName = args[1]
    log('table')
    return db.table(tableName)
  }

  filter (args) {
    let sequence = this.evaluate(args[0])
    let predicate = null
    if (Array.isArray(args[1])) {
      predicate = this.evaluate(args[1])
    } else {
      predicate = args[1]
    }
    log('filter')
    return sequence.filter(predicate)
  }

  reduce (args) {
    let sequence = this.evaluate(args[0])
    let predicate = this.evaluate(args[1])
    log('reduce()')
    return sequence.reduce(predicate)
  }

  map (args) {
    let sequence = this.evaluate(args[0])
    let predicate = this.evaluate(args[1])
    log('map()')
    return sequence.map(predicate)
  }

  concatMap (args) {
    let sequence = this.evaluate(args.shift())
    let predicate = this.evaluate(...args)
    log('concatMap()')
    return sequence.concatMap(predicate)
  }

  orderBy (args) {
    let sequence = this.evaluate(args.shift())
    return sequence.orderBy(...args)
  }

  add (args) {
    let var1 = this.evaluate(args[0])
    let var2 = this.evaluate(args[1])
    log('add()', var1, var2)
    return var1.add(var2)
  }

  sub (args) {
    let var1 = this.evaluate(args[0])
    let var2 = this.evaluate(args[1])
    log('sub()', var1, var2)
    return var1.sub(var2)
  }

  mul (args) {
    let var1 = this.evaluate(args[0])
    let var2 = this.evaluate(args[1])
    log('mul()', var1, var2)
    return var1.mul(var2)
  }

  div (args) {
    let var1 = this.evaluate(args[0])
    let var2 = this.evaluate(args[1])
    log('div()', var1, var2)
    return var1.div(var2)
  }

  mod (args) {
    let var1 = this.evaluate(args[0])
    let var2 = this.evaluate(args[1])
    log('mod()', var1, var2)
    return var1.mod(var2)
  }

  expr (args) {
    return r.expr(args)
  }

  count (args) {
    let sequence = this.evaluate(args[0])
    return sequence.count()
  }

  func (args) {
    this.fnArgs = args[0][1].map((arg) => `var_${arg}`)
    this.funcBody = args[1]
    let vars = ''
    this.fnArgs.forEach(arg => {
      vars = vars + `this.${arg} = ${arg};`
    })
    log('func', vars)
    return new Function(
      this.fnArgs.join(','),
      `
        ${vars};
        return this.evaluate(this.funcBody);
      `
    ).bind(this)
  }

  makeArray (args) {
    args = args.map(arg => {
      if (arg[0] === 2 && Array.isArray(arg[1])) {
        return this.evaluate(arg)
      } else {
        return arg
      }
    })
    log(`makeArray ${args}`)
    return r(args)
  }

  bracket (args) {
    let sequence = this.evaluate(args[0])
    log(`.(${args[1]})`)
    return sequence(args[1])
  }

  varId (args) {
    log(`${args}`)
    return this[`var_${args}`]
  }

  withFields (args) {
    let sequence = this.evaluate(args.shift())
    return sequence.withFields(...args)
  }

  // Math and logic
  and (args) {
    let var1 = this.evaluate(args.shift())
    return var1.and(...args)
  }

  or (args) {
    let var1 = this.evaluate(args.shift())
    return var1.or(...args)
  }

  eq (args) {
    let var1 = this.evaluate(args.shift())
    return var1.eq(...args)
  }

  ne (args) {
    let var1 = this.evaluate(args.shift())
    return var1.ne(...args)
  }

  gt (args) {
    let var1 = this.evaluate(args.shift())
    return var1.gt(...args)
  }

  ge (args) {
    let var1 = this.evaluate(args.shift())
    return var1.ge(...args)
  }

  lt (args) {
    let var1 = this.evaluate(args.shift())
    return var1.lt(...args)
  }

  le (args) {
    let var1 = this.evaluate(args.shift())
    return var1.le(...args)
  }

  not (args) {
    let var1 = this.evaluate(args.shift())
    return var1.not()
  }

  random (args) {
    return r.random(...args)
  }

  // Dates and times

  now () {
    return r.now()
  }

  time (args) {
    return r.time(...args)
  }

  epochTime(args) {
    return r.epochTime(...args)
  }

  ISO8601(args) {
    return r.ISO8601(...args)
  }

  inTimezone(args) {
    let var1 = this.evaluate(args.shift())
    return var1.inTimezone(...args)
  }

  timezone(args) {
    let var1 = this.evaluate(args.shift())
    return var1.timezone(...args)
  }

  during(args) {
    let var1 = this.evaluate(args.shift())
    let var2 = this.evaluate(args.shift())
    let var3 = this.evaluate(args.shift())
    return var1.during(var2, var3)
  }

  date (args) {
    let var1 = this.evaluate(args.shift())
    return var1.date()
  }

  timeOfDay (args) {
    let var1 = this.evaluate(args.shift())
    return var1.timeOfDay()
  }

  year (args) {
    let var1 = this.evaluate(args.shift())
    return var1.year()
  }

  month (args) {
    let var1 = this.evaluate(args.shift())
    return var1.month()
  }

  day (args) {
    let var1 = this.evaluate(args.shift())
    return var1.day()
  }

  dayOfWeek (args) {
    let var1 = this.evaluate(args.shift())
    return var1.dayOfWeek()
  }

  dayOfYear (args) {
    let var1 = this.evaluate(args.shift())
    return var1.dayOfYear()
  }

  hours(args) {
    let var1 = this.evaluate(args.shift())
    return var1.hours()
  }

  minutes(args) {
    let var1 = this.evaluate(args.shift())
    return var1.minutes()
  }

  seconds(args) {
    let var1 = this.evaluate(args.shift())
    return var1.seconds()
  }

  toISO8601 (args) {
    let var1 = this.evaluate(args.shift())
    return var1.toISO8601()
  }

  toEpochTime (args) {
    let var1 = this.evaluate(args.shift())
    return var1.toEpochTime()
  }

  skip (args) {
    let var1 = this.evaluate(args.shift())
    return var1.skip(...args)
  }

  limit (args) {
    let sequence = this.evaluate(args.shift())
    return sequence.limit(...args)
  }

  slice (args) {
    let var1 = this.evaluate(args.shift())
    return var1.slice(...args)
  }

  nth (args) {
    let var1 = this.evaluate(args.shift())
    return var1.nth(...args)
  }

  offsetsOf (args) {
    let var1 = this.evaluate(args.shift())
    return var1.offsetsOf(...args)
  }

  isEmpty (args) {
    let var1 = this.evaluate(args.shift())
    return var1.isEmpty()
  }

  union (args) {
    let var1 = this.evaluate(args.shift())
    let var2s = args.map(arg => this.evaluate(arg))
    return var1.union(...var2s)
  }

  sample (args) {
    let var1 = this.evaluate(args.shift())
    return var1.sample(...args)
  }

}

export default Query
