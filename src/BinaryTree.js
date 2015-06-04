class BinaryTree {
  constructor (value, left, right) {
    this.value = value
    this.left = left
    this.right = right
  }

  postorder (f) {
    this.walk(f, ['left', 'right', 'this'])
  }

  preorder (f) {
    this.walk(f, ['this', 'left', 'right'])
  }

  inorder (f) {
    this.walk(f, ['left', 'this', 'right'])
  }

  levelorder (f) {
    let queue = [this]
    while(queue.length != 0) {
      let node = queue.shift()
      f(node.value)
      if (node.left) queue.push(node.left)
      if (node.right) queue.push(node.right)
    }
  }

  walk (func, order) {
    for (let i = 0 ; i < order.length ; i += 1) {
      switch (order[i]) {
        case 'this': func(this.value) ; break
        case 'left': if (this.left) this.left.walk(func, order) ; break
        case 'right': if (this.right) this.right.walk(func, order) ; break
      }
    }
  }
}

export default BinaryTree
