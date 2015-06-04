import rethinkdbdash from 'rethinkdbdash'
import co from 'co'

let r = rethinkdbdash({
  host: '192.168.100.5',
  port: 28015
})

co(function * () {
  console.time('mapreduce')
  let result = yield r.db('test').table('bills').map((bill) => {
    return 1
  }).reduce((left, right) => {
    return left.add(right)
  }).run()
  console.timeEnd('mapreduce')
  console.log(result)
})

