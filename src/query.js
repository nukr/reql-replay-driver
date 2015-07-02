import rethinkdbdash from 'rethinkdbdash'
import protodef from './protodef'
import Debug from 'debug'
import config from '../config.js'

let debug = Debug('reql:query')
let termTypes = protodef.Term.TermType
let r = null

class Query {
  constructor (query, dbName) {
    if (r === null) throw Error('db not setting yet please set by Query.options(dbOptions)')
    this.dbName = dbName || 'test'
    this.query = query
    this.fnId = 0
    this.fnArgs = {}
  }

  static options (dbOptions) {
    r = rethinkdbdash(dbOptions)
  }

  run (query) {
    query = query || this.query
    let result = this.evaluate(query)
    return result.run()
    debug('result', result)
  }

  evaluate (term, internalOptions) {
    internalOptions = internalOptions || {}

    if(!Array.isArray(term)) {
      return r.expr(term)
    }

    let termType = term[0]
    switch (termType) {
      case termTypes.DB: // 14
        return this.db(term[1], internalOptions)
      case termTypes.TABLE: // 15
        return this.table(term[1], internalOptions)
      case termTypes.FILTER: // 39
        return this.filter(term[1], internalOptions)
      case termTypes.FUNC: // 69
        return this.func(term[1], internalOptions)
      case termTypes.FUNCALL: // 64
        return this.funCall(term[1], internalOptions)
      case termTypes.MAKE_ARRAY: // 2
        return this.makeArray(term[1], internalOptions)
      case termTypes.VAR: // 10
        return this.varId(term[1], internalOptions)
      case termTypes.BRACKET: // 170
        return this.bracket(term[1], internalOptions)
      case termTypes.REDUCE: // 37
        return this.reduce(term[1], internalOptions)
      case termTypes.MAP: // 38
        return this.map(term[1], internalOptions)
      case termTypes.ADD: // 24
        return this.add(term[1], internalOptions)
      case termTypes.SUB:
        return this.sub(term[1], internalOptions)
      case termTypes.MUL:
        return this.mul(term[1], internalOptions)
      case termTypes.DIV:
        return this.div(term[1], internalOptions)
      case termTypes.MOD:
        return this.mod(term[1], internalOptions)
      case termTypes.LIMIT: // 71
        return this.limit(term[1], internalOptions)
      case termTypes.WITH_FIELDS: // 96
        return this.withFields(term[1], internalOptions)
      case termTypes.COUNT:
        return this.count(term[1], internalOptions)
      case termTypes.AND:
        return this.and(term[1], internalOptions)
      case termTypes.OR:
        return this.or(term[1], internalOptions)
      case termTypes.EQ:
        return this.eq(term[1], internalOptions)
      case termTypes.NE:
        return this.ne(term[1], internalOptions)
      case termTypes.GT:
        return this.gt(term[1], internalOptions)
      case termTypes.GE:
        return this.ge(term[1], internalOptions)
      case termTypes.LT:
        return this.lt(term[1], internalOptions)
      case termTypes.LE:
        return this.le(term[1], internalOptions)
      case termTypes.NOT:
        return this.not(term[1], internalOptions)
      case termTypes.RANDOM:
        return this.random(term[1], internalOptions)
      case termTypes.NOW:
        return this.now(term[1], internalOptions)
      case termTypes.TIME:
        return this.time(term[1], internalOptions)
      case termTypes.EPOCH_TIME:
        return this.epochTime(term[1], internalOptions)
      case termTypes.ISO8601:
        return this.ISO8601(term[1], internalOptions)
      case termTypes.IN_TIMEZONE:
        return this.inTimezone(term[1], internalOptions)
      case termTypes.TIMEZONE:
        return this.timezone(term[1], internalOptions)
      case termTypes.DURING:
        return this.during(term[1], internalOptions)
      case termTypes.DATE:
        return this.date(term[1], internalOptions)
      case termTypes.TIME_OF_DAY:
        return this.timeOfDay(term[1], internalOptions)
      case termTypes.YEAR:
        return this.year(term[1], internalOptions)
      case termTypes.MONTH:
        return this.month(term[1], internalOptions)
      case termTypes.DAY:
        return this.day(term[1], internalOptions)
      case termTypes.DAY_OF_WEEK:
        return this.dayOfWeek(term[1], internalOptions)
      case termTypes.DAY_OF_YEAR:
        return this.dayOfYear(term[1], internalOptions)
      case termTypes.HOURS:
        return this.hours(term[1], internalOptions)
      case termTypes.MINUTES:
        return this.minutes(term[1], internalOptions)
      case termTypes.SECONDS:
        return this.seconds(term[1], internalOptions)
      case termTypes.TO_ISO8601:
        return this.toISO8601(term[1], internalOptions)
      case termTypes.TO_EPOCH_TIME:
        return this.toEpochTime(term[1], internalOptions)
      case termTypes.CONCAT_MAP:
        return this.concatMap(term[1], internalOptions)
      case termTypes.ORDER_BY:
        return this.orderBy(term[1], internalOptions)
      case termTypes.SKIP:
        return this.skip(term[1], internalOptions)
      case termTypes.SLICE:
        return this.slice(term[1], internalOptions)
      case termTypes.NTH:
        return this.nth(term[1], internalOptions)
      case termTypes.OFFSETS_OF:
        return this.offsetsOf(term[1], internalOptions)
      case termTypes.IS_EMPTY:
        return this.isEmpty(term[1], internalOptions)
      case termTypes.UNION:
        return this.union(term[1], internalOptions)
      case termTypes.SAMPLE:
        return this.sample(term[1], internalOptions)
      case termTypes.GROUP:
        return this.group(term[1], internalOptions)
      case termTypes.UNGROUP:
        return this.ungroup(term[1], internalOptions)
      case termTypes.INFO:
        return this.info(term[1], internalOptions)
      case termTypes.SUM:
        return this.sum(term[1], internalOptions)
      case termTypes.AVG:
        return this.avg(term[1], internalOptions)
      case termTypes.MIN:
        return this.min(term[1], internalOptions)
      case termTypes.MAX:
        return this.max(term[1], internalOptions)
      case termTypes.DISTINCT:
        return this.distinct(term[1], internalOptions)
      case termTypes.CONTAINS:
        return this.contains(term[1], internalOptions)
      case termTypes.IMPLICIT_VAR:
        return this.row(term[1], internalOptions)
      case termTypes.PLUCK:
        return this.pluck(term[1], internalOptions)
      case termTypes.WITHOUT:
        return this.without(term[1], internalOptions)
      case termTypes.MERGE:
        return this.merge(term[1], internalOptions)
      case termTypes.APPEND:
        return this.append(term[1], internalOptions)
      case termTypes.PREPEND:
        return this.prepend(term[1], internalOptions)
      case termTypes.DIFFERENCE:
        return this.difference(term[1], internalOptions)
      case termTypes.SET_INSERT:
        return this.setInsert(term[1], internalOptions)
      case termTypes.SET_UNION:
        return this.setUnion(term[1], internalOptions)
      case termTypes.SET_INTERSECTION:
        return this.setIntersection(term[1], internalOptions)
      case termTypes.SET_DIFFERENCE:
        return this.setDifference(term[1], internalOptions)
      case termTypes.GET_FIELD:
        return this.getField(term[1], internalOptions)
      case termTypes.HAS_FIELDS:
        return this.hasFields(term[1], internalOptions)
      case termTypes.INSERT_AT:
        return this.insertAt(term[1], internalOptions)
      case termTypes.SPLICE_AT:
        return this.spliceAt(term[1], internalOptions)
      case termTypes.DELETE_AT:
        return this.deleteAt(term[1], internalOptions)
      case termTypes.CHANGE_AT:
        return this.changeAt(term[1], internalOptions)
      case termTypes.KEYS:
        return this.keys(term[1], internalOptions)
      case termTypes.LITERAL:
        return this.literal(term[1], internalOptions)
      case termTypes.OBJECT:
        return this.object(term[1], internalOptions)
      case termTypes.INSERT:
        return this.insert(term[1], internalOptions)
      case termTypes.UPDATE:
        return this.update(term[1], internalOptions)
      case termTypes.DELETE:
        return this.delete(term[1], internalOptions)
      case termTypes.REPLACE:
        return this.replace(term[1], internalOptions)
      case termTypes.GET:
        return this.get(term[1], internalOptions)
      case termTypes.GET_ALL:
        return this.getAll(term[1], term[2], internalOptions)
      case termTypes.COERCE_TO:
        return this.coerceTo(term[1], internalOptions)
      case termTypes.MATCH:
        return this.match(term[1], internalOptions)
      case termTypes.SPLIT:
        return this.split(term[1], internalOptions)
      case termTypes.UPCASE:
        return this.upcase(term[1], internalOptions)
      case termTypes.DOWNCASE:
        return this.downcase(term[1], internalOptions)
      case termTypes.BETWEEN:
        return this.between(term[1], term[2], internalOptions)
      case termTypes.ARGS:
        return this.args(term[1], internalOptions)
      case termTypes.BRANCH:
        return this.branch(term[1], internalOptions)
      case termTypes.RANGE:
        return this.range(term[1], internalOptions)
      case termTypes.ERROR:
        return this.error(term[1], internalOptions)
      case termTypes.DEFAULT:
        return this.default(term[1], internalOptions)
      case termTypes.TYPE_OF:
        return this.typeOf(term[1], internalOptions)
      case termTypes.JSON:
        return this.json(term[1], internalOptions)
      case termTypes.TO_JSON_STRING:
        return this.toJsonString(term[1], internalOptions)
      case termTypes.HTTP:
        return this.http(term[1], internalOptions)
      case termTypes.UUID:
        return this.uuid(term[1], internalOptions)
      case termTypes.FOR_EACH:
        return this.foreach(term[1], internalOptions)
      case termTypes.INNER_JOIN:
        throw new Error('illegal command [innerJoin]')
      default:
        throw new Error('unknown term')
    }
  }

  db (args, options) {
    return r.db(this.dbName)
  }

  table (args, options) {
    let db = this.evaluate(args[0], options)
    let tableName = args[1]
    return db.table(tableName)
  }

  filter (args, options) {
    let sequence = this.evaluate(args[0], options)
    let predicate = null
    if (Array.isArray(args[1])) {
      predicate = this.evaluate(args[1], options)
    } else {
      predicate = args[1]
    }
    return sequence.filter(predicate)
  }

  reduce (args, options) {
    let sequence = this.evaluate(args[0], options)
    let predicate = this.evaluate(args[1], options)
    return sequence.reduce(predicate)
  }

  map (args, options) {
    let sequence = this.evaluate(args[0], options)
    let predicate = this.evaluate(args[1], options)
    return sequence.map(predicate)
  }

  concatMap (args, options) {
    let sequence = this.evaluate(args.shift(), options)
    let predicate = this.evaluate(args[0], options)
    return sequence.concatMap(predicate)
  }

  orderBy (args, options) {
    let sequence = this.evaluate(args.shift(), options)
    return sequence.orderBy(...args)
  }

  add (args, options) {
    let var1 = this.evaluate(args[0], options)
    let var2 = this.evaluate(args[1], options)
    return var1.add(var2)
  }

  sub (args, options) {
    let var1 = this.evaluate(args[0], options)
    let var2 = this.evaluate(args[1], options)
    return var1.sub(var2)
  }

  mul (args, options) {
    let var1 = this.evaluate(args[0], options)
    let var2 = this.evaluate(args[1], options)
    return var1.mul(var2)
  }

  div (args, options) {
    let var1 = this.evaluate(args[0], options)
    let var2 = this.evaluate(args[1], options)
    return var1.div(var2)
  }

  mod (args, options) {
    let var1 = this.evaluate(args[0], options)
    let var2 = this.evaluate(args[1], options)
    return var1.mod(var2)
  }

  expr (args, options) {
    return r.expr(args, options)
  }

  count (args, options) {
    let sequence = this.evaluate(args[0], options)
    return sequence.count()
  }

  func (args, options) {
    this.fnId++
    this.fnArgs[this.fnId] = args[0][1].map((arg) => `var_${arg}`)
    this.funcBody = args[1]

    let vars = ''
    this.fnArgs[this.fnId].forEach(arg => {
      vars += `this.${arg} = ${arg};`
    })

    return new Function(
      this.fnArgs[this.fnId].join(','),
      `
        ${vars};
        var fnId = this.fnId;
        return this.evaluate(this.funcBody, fnId);
      `
    ).bind(this)
  }

  funCall (args, options) {
    let sequence = this.evaluate(args[1], options)
    return sequence.do(this.evaluate(args[0]))
  }

  makeArray (args, options) {
    args = args.map(arg => {
      if (arg[0] === 2 && Array.isArray(arg[1])) {
        return this.evaluate(arg, options)
      } else {
        return arg
      }
    })
    return r(args, options)
  }

  bracket (args, options) {
    let sequence = this.evaluate(args[0], options)
    return sequence(args[1])
  }

  varId (args, options) {
    return this[`var_${args}`]
  }

  withFields (args, options) {
    let sequence = this.evaluate(args.shift(), options)
    return sequence.withFields(...args)
  }

  // Math and logic
  and (args, options) {
    let var1 = this.evaluate(args.shift(), options)
    return var1.and(...args)
  }

  or (args, options) {
    let var1 = this.evaluate(args.shift(), options)
    return var1.or(...args)
  }

  eq (args, options) {
    let var1 = this.evaluate(args.shift(), options)
    return var1.eq(...args)
  }

  ne (args, options) {
    let var1 = this.evaluate(args.shift(), options)
    return var1.ne(...args)
  }

  gt (args, options) {
    let var1 = this.evaluate(args.shift(), options)
    return var1.gt(...args)
  }

  ge (args, options) {
    let var1 = this.evaluate(args.shift(), options)
    return var1.ge(...args)
  }

  lt (args, options) {
    let var1 = this.evaluate(args.shift(), options)
    return var1.lt(...args)
  }

  le (args, options) {
    let var1 = this.evaluate(args.shift(), options)
    return var1.le(...args)
  }

  not (args, options) {
    let var1 = this.evaluate(args.shift(), options)
    return var1.not()
  }

  random (args, options) {
    return r.random(...args)
  }

  // Dates and times

  now () {
    return r.now()
  }

  time (args, options) {
    return r.time(...args)
  }

  epochTime(args, options) {
    return r.epochTime(...args)
  }

  ISO8601(args, options) {
    return r.ISO8601(...args)
  }

  inTimezone(args, options) {
    let var1 = this.evaluate(args.shift(), options)
    return var1.inTimezone(...args)
  }

  timezone(args, options) {
    let var1 = this.evaluate(args.shift(), options)
    return var1.timezone(...args)
  }

  during(args, options) {
    let var1 = this.evaluate(args.shift(), options)
    let var2 = this.evaluate(args.shift(), options)
    let var3 = this.evaluate(args.shift(), options)
    return var1.during(var2, var3)
  }

  date (args, options) {
    let var1 = this.evaluate(args.shift(), options)
    return var1.date()
  }

  timeOfDay (args, options) {
    let var1 = this.evaluate(args.shift(), options)
    return var1.timeOfDay()
  }

  year (args, options) {
    let var1 = this.evaluate(args.shift(), options)
    return var1.year()
  }

  month (args, options) {
    let var1 = this.evaluate(args.shift(), options)
    return var1.month()
  }

  day (args, options) {
    let var1 = this.evaluate(args.shift(), options)
    return var1.day()
  }

  dayOfWeek (args, options) {
    let var1 = this.evaluate(args.shift(), options)
    return var1.dayOfWeek()
  }

  dayOfYear (args, options) {
    let var1 = this.evaluate(args.shift(), options)
    return var1.dayOfYear()
  }

  hours(args, options) {
    let var1 = this.evaluate(args.shift(), options)
    return var1.hours()
  }

  minutes(args, options) {
    let var1 = this.evaluate(args.shift(), options)
    return var1.minutes()
  }

  seconds(args, options) {
    let var1 = this.evaluate(args.shift(), options)
    return var1.seconds()
  }

  toISO8601 (args, options) {
    let var1 = this.evaluate(args.shift(), options)
    return var1.toISO8601()
  }

  toEpochTime (args, options) {
    let var1 = this.evaluate(args.shift(), options)
    return var1.toEpochTime()
  }

  skip (args, options) {
    let var1 = this.evaluate(args.shift(), options)
    return var1.skip(...args)
  }

  limit (args, options) {
    let sequence = this.evaluate(args.shift(), options)
    return sequence.limit(...args)
  }

  slice (args, options) {
    let var1 = this.evaluate(args.shift(), options)
    return var1.slice(...args)
  }

  nth (args, options) {
    let var1 = this.evaluate(args.shift(), options)
    return var1.nth(...args)
  }

  offsetsOf (args, options) {
    let var1 = this.evaluate(args.shift(), options)
    return var1.offsetsOf(...args)
  }

  isEmpty (args, options) {
    let var1 = this.evaluate(args.shift(), options)
    return var1.isEmpty()
  }

  union (args, options) {
    let var1 = this.evaluate(args.shift(), options)
    let var2s = args.map(arg => this.evaluate(arg, options))
    return var1.union(...var2s)
  }

  sample (args, options) {
    let var1 = this.evaluate(args.shift(), options)
    return var1.sample(...args)
  }

  group (args, options) {
    let sequence = this.evaluate(args.shift(), options)
    return sequence.group(...args)
  }

  ungroup (args, options) {
    let grouped_stream = this.evaluate(args.shift(), options)
    return grouped_stream.ungroup()
  }

  sum (args, options) {
    let sequence = this.evaluate(args.shift(), options)
    return sequence.sum()
  }

  avg (args, options) {
    let sequence = this.evaluate(args.shift(), options)
    return sequence.avg()
  }

  min (args, options) {
    let sequence = this.evaluate(args.shift(), options)
    return sequence.min()
  }

  max (args, options) {
    let sequence = this.evaluate(args.shift(), options)
    return sequence.max()
  }

  distinct (args, options) {
    let sequence = this.evaluate(args.shift(), options)
    return sequence.distinct()
  }

  contains (args, options) {
    let sequence = this.evaluate(args.shift(), options)
    return sequence.contains(...args)
  }

  info (args, options) {
    let any = this.evaluate(args.shift(), options)
    return any.info()
  }

  row (args, options) {
    return this[`${this.fnArgs[options]}`]
  }

  pluck (args, options) {
    let sequence = this.evaluate(args.shift(), options)
    return sequence.pluck(...args)
  }

  without (args, options) {
    let sequence = this.evaluate(args.shift(), options)
    return sequence.without(...args)
  }

  merge (args, options) {
    let sequence = this.evaluate(args.shift(), options)
    return sequence.merge(...args)
  }

  append (args, options) {
    let sequence = this.evaluate(args.shift(), options)
    return sequence.append(...args)
  }

  prepend (args, options) {
    let sequence = this.evaluate(args.shift(), options)
    return sequence.prepend(...args)
  }

  difference (args, options) {
    let sequence = this.evaluate(args.shift(), options)
    return sequence.difference(this.evaluate(args.shift(), options))
  }

  setInsert (args, options) {
    let sequence = this.evaluate(args.shift(), options)
    return sequence.setInsert(...args)
  }

  setUnion (args, options) {
    let sequence = this.evaluate(args.shift(), options)
    return sequence.setUnion(this.evaluate(args.shift(), options))
  }

  setIntersection (args, options) {
    let sequence = this.evaluate(args.shift(), options)
    return sequence.setIntersection(this.evaluate(args.shift(), options))
  }

  setDifference (args, options) {
    let sequence = this.evaluate(args.shift(), options)
    return sequence.setDifference(this.evaluate(args.shift(), options))
  }

  getField (args, options) {
    let sequence = this.evaluate(args.shift(), options)
    return sequence.getField(...args)
  }

  hasFields (args, options) {
    let sequence = this.evaluate(args.shift(), options)
    return sequence.hasFields(this.evaluate(args.shift(), options))
  }

  insertAt (args, options) {
    let sequence = this.evaluate(args.shift(), options)
    return sequence.insertAt(...args)
  }

  spliceAt (args, options) {
    let sequence = this.evaluate(args.shift(), options)
    return sequence.spliceAt(args.shift(), this.evaluate(args.shift(), options))
  }

  deleteAt (args, options) {
    let sequence = this.evaluate(args.shift(), options)
    return sequence.deleteAt(...args)
  }

  changeAt (args, options) {
    let sequence = this.evaluate(args.shift(), options)
    return sequence.changeAt(...args)
  }

  keys (args, options) {
    let sequence = this.evaluate(args.shift(), options)
    return sequence.keys()
  }

  literal (args, options) {
    return r.literal(args)
  }

  object (args, options) {
    return r.object(...args)
  }

  insert (args, options) {
    let table = this.evaluate(args.shift(), options)
    return table.insert(...args)
  }

  update (args, options) {
    let table = this.evaluate(args.shift(), options)
    return table.update(...args)
  }

  delete (args, options) {
    let table = this.evaluate(args.shift(), options)
    return table.delete()
  }

  replace (args, options) {
    let table = this.evaluate(args.shift(), options)
    return table.replace(...args)
  }

  get (args, options) {
    let table = this.evaluate(args.shift(), options)
    return table.get(...args)
  }

  getAll (args, index, options) {
    let table = this.evaluate(args.shift(), options)
    return table.getAll(...args, index)
  }

  between (args, optionalArgs, options) {
    let table = this.evaluate(args.shift(), options)
    return table.between(...args, optionalArgs)
  }

  coerceTo (args, options) {
    let sequence = this.evaluate(args.shift(), options)
    return sequence.coerceTo(...args)
  }

  match (args, options) {
    let sequence = this.evaluate(args.shift(), options)
    return sequence.match(...args)
  }

  split (args, options) {
    let sequence = this.evaluate(args.shift(), options)
    return sequence.split(...args)
  }

  upcase (args, options) {
    let sequence = this.evaluate(args.shift(), options)
    return sequence.upcase(...args)
  }

  downcase (args, options) {
    let sequence = this.evaluate(args.shift(), options)
    return sequence.downcase(...args)
  }

  args (args, options) {
    return r.args(args[0][1])
  }

  branch (args, options) {
    return r.branch(
      this.evaluate(args.shift(), options),
      this.evaluate(args.shift(), options),
      this.evaluate(args.shift(), options)
    )
  }

  range (args, options) {
    return r.range(args.shift())
  }

  error (args, options) {
    return r.error(args.shift())
  }

  default (args, options) {
    return this.evaluate(args.shift(), options).default(args.shift())
  }

  typeOf (args, options) {
    return this.evaluate(args.shift(), options).typeOf(args.shift())
  }

  json (args, options) {
    return r.json(args.shift())
  }

  toJsonString (args, options) {
    return this.evaluate(args.shift(), options).toJsonString()
  }

  http (args, options) {
    return r.http(...args)
  }

  uuid (args, options) {
    return r.uuid()
  }

  foreach (args, options) {
    let sequence = this.evaluate(args[0], options)
    let predicate = this.evaluate(args[1], options)
    return sequence.forEach(predicate)
  }

}

export default Query
