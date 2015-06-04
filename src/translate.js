import protodef from './reverse-protodef'

export default (number) => {
  return protodef.Term.TermType[number];
}
