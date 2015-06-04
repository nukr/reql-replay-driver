let isTree = (tree) => {
  let left = null
  let right = null
  let value = null
  if (typeof tree !== 'undefined' && tree !== null) {
    let keys = Object.keys(tree)

    keys.forEach((key) => {
      switch (key) {
        case 'left':
          left = true
          break
        case 'right':
          right = true
          break
        case 'value':
          value = true
          break
      }
    })
  }

  if (left && right && value) {
    return true
  } else {
    return false
  }
}

let extractTree = (tree) => {
  let arr = []
  if (Array.isArray(tree.value)) {
    arr.push({array: tree.value})
  } else {
    arr.push(tree.value)
  }

  if (isTree(tree.left) && tree.right !== null) {
    arr.unshift(extractTree(tree.left))
  } else if (isTree(tree.left) && tree.right === null) {
    arr.push(extractTree(tree.left))
  }

  if (isTree(tree.right)) {
    arr.push(extractTree(tree.right))
  }
  return arr
}

