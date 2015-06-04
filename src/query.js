import rethinkdbdash from 'rethinkdbdash'
import protodef from './protodef'
import debug from 'debug'

let log = debug('query')

let termTypes = protodef.Term.TermType
let r = rethinkdbdash({
  host: '192.168.100.5',
  port: 28015
})

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
      case termTypes.HOURS:
        return this.hours(term[1])
      case termTypes.DATE:
        return this.date(term[1])
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
    this.fnArgs = this.evaluate(args[0])
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
    log(`makeArray ${args}`)
    return args.map((arg) => `var_${arg}`)
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

  limit (args) {
    let sequence = this.evaluate(args[0])
    return sequence.limit(args[1])
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

  hours(args) {
    let var1 = this.evaluate(args.shift())
    return var1.hours()
  }
}

export default Query
