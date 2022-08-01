import { modelRenderConfig } from './config'
class Tree {
  constructor (options) {
    this.boxW = modelRenderConfig.tableBoxWidth
    this.boxH = modelRenderConfig.tableBoxHeight
    this.boxML = modelRenderConfig.tableBoxLeft
    this.boxMT = modelRenderConfig.tableBoxTop
    this.josnId = 0
    this.rootGuid = options.rootGuid
    this.showLinkCons = options.showLinkCons
    if (!this.rootGuid) {
      throw new Error('rootGuid is required for Tree Layout')
    }
    if (!this.showLinkCons) {
      throw new Error('showLinkCons is required for Tree Layout')
    }
    this.nodeDB = new NodeDB(this.getNodeStructure(), this)
  }

  getNodeStructure (root) {
    this.josnId++
    const rootNode = root || {guid: this.rootGuid, _json_id: 0, children: []}
    for (let i in this.showLinkCons) {
      if (rootNode.guid === i.split('$')[1]) {
        const node = {guid: i.split('$')[0], _json_id: this.josnId, children: []}
        rootNode.children.push(node)
        this.getNodeStructure(node)
      }
    }
    return rootNode
  }

  getNodeDb () {
    return this.nodeDB
  }

  positionTree () {
    const rootNode = this.root()
    this.resetLevelData()
    this.firstWalk(rootNode, 0)
    this.secondWalk(rootNode, 0, 0, 0)

    return this
  }

  firstWalk (node, level) {
    node.prelim = null
    node.modifier = null
    this.setNeighbors(node, level)
    const leftSibling = node.leftSibling()

    if (node.children.length === 0) {
      // set preliminary x-coordinate
      if (leftSibling) {
        node.prelim = leftSibling.prelim + this.boxW + this.boxML
      } else {
        node.prelim = 0
      }
    } else {
      // node is not a leaf,  firstWalk for each child
      for (let i = 0; i < node.children.length; i++) {
        this.firstWalk(node.childAt(i), level + 1)
      }

      const midPoint = node.childrenCenter(this.boxW) - this.boxW / 2

      if (leftSibling) {
        node.prelim = leftSibling.prelim + this.boxW + this.boxML
        node.modifier = node.prelim - midPoint
        this.apportion(node, level)
      } else {
        node.prelim = midPoint
      }
    }
    return this
  }

  apportion (node, level) {
    let firstChild = node.firstChild()
    let firstChildLeftNeighbor = firstChild.leftNeighbor()
    let compareDepth = 1

    while (firstChild && firstChildLeftNeighbor) {
      // calculate the position of the firstChild, according to the position of firstChildLeftNeighbor
      let modifierSumRight = 0
      let modifierSumLeft = 0
      let leftAncestor = firstChildLeftNeighbor
      let rightAncestor = firstChild

      for (let i = 0; i < compareDepth; i++) {
        leftAncestor = leftAncestor.parent()
        rightAncestor = rightAncestor.parent()
        modifierSumLeft += leftAncestor.modifier
        modifierSumRight += rightAncestor.modifier
      }

      // find the gap between two trees and apply it to subTrees
      // and mathing smaller gaps to smaller subtrees
      let totalGap = firstChildLeftNeighbor.prelim + modifierSumLeft + this.boxW + this.boxML - (firstChild.prelim + modifierSumRight)

      if (totalGap > 0) {
        let subtreeAux = node
        let numSubtrees = 0

        // count all the subtrees in the LeftSibling
        while (subtreeAux && subtreeAux.id !== leftAncestor.id) {
          subtreeAux = subtreeAux.leftSibling()
          numSubtrees++
        }

        if (subtreeAux) {
          let subtreeMoveAux = node
          const singleGap = totalGap / numSubtrees

          while (subtreeMoveAux.id !== leftAncestor.id) {
            subtreeMoveAux.prelim += totalGap
            subtreeMoveAux.modifier += totalGap

            totalGap -= singleGap
            subtreeMoveAux = subtreeMoveAux.leftSibling()
          }
        }
      }
      compareDepth++

      firstChild = (firstChild.children.length === 0) ? node.leftMost(0, compareDepth) : firstChild.firstChild()

      if (firstChild) {
        firstChildLeftNeighbor = firstChild.leftNeighbor()
      }
    }
  }

  secondWalk (node, level, X, Y) {
    const xTmp = node.prelim + X
    const yTmp = Y
    node.X = xTmp
    node.Y = yTmp

    if (node.children.length !== 0) {
      this.secondWalk(node.firstChild(), level + 1, X + node.modifier, Y + this.boxH + this.boxMT)
    }

    if (node.rightSibling()) {
      this.secondWalk(node.rightSibling(), level, X, Y)
    }
  }

  setNeighbors (node, level) {
    node.leftNeighborId = this.lastNodeOnLevel[level]
    if (node.leftNeighborId) {
      node.leftNeighbor().rightNeighborId = node.id
    }
    this.lastNodeOnLevel[level] = node.id
    return this
  }

  resetLevelData () {
    this.lastNodeOnLevel = []
    return this
  }

  root () {
    return this.nodeDB.get(0)
  }
}

class NodeDB {
  constructor (nodeStructure, tree) {
    this.db = []
    const self = this
    const iterateChildren = function (node, parentId) {
      const newNode = self.createNode(node, parentId, tree)
      if (node.children) {
        for (let i = 0; i < node.children.length; i++) {
          iterateChildren(node.children[i], newNode.id)
        }
      }
    }
    iterateChildren(nodeStructure, -1)
    return this
  }

  get (nodeId) {
    return this.db[nodeId]
  }

  createNode (nodeStructure, parentId, tree) {
    const node = new TreeNode(nodeStructure, this.db.length, parentId, tree)
    this.db.push(node)
    // skip root node (0)
    if (parentId >= 0) {
      const parent = this.get(parentId)
      parent.children.push(node.id)
    }
    return node
  }
}

class TreeNode {
  constructor (nodeStructure, id, parentId, tree) {
    this.id = id
    this.guid = nodeStructure.guid
    this.parentId = parentId
    this.tree = tree
    this.prelim = 0
    this.modifier = 0
    this.leftNeighborId = null
    this.children = []

    return this
  }

  dbGet (nodeId) {
    return this.getTreeNodeDb().get(nodeId)
  }

  childAt (index) {
    return this.dbGet(this.children[index])
  }

  firstChild () {
    return this.childAt(0)
  }

  lastChild () {
    return this.childAt(this.children.length - 1)
  }

  getTreeNodeDb () {
    return this.tree.getNodeDb()
  }

  lookupNode (nodeId) {
    return this.getTreeNodeDb().get(nodeId)
  }

  parent () {
    return this.lookupNode(this.parentId)
  }

  leftNeighbor () {
    if (this.leftNeighborId) {
      return this.lookupNode(this.leftNeighborId)
    }
  }

  leftSibling () {
    const leftNeighbor = this.leftNeighbor()
    if (leftNeighbor && leftNeighbor.parentId === this.parentId) {
      return leftNeighbor
    }
  }

  rightNeighbor () {
    if (this.rightNeighborId) {
      return this.lookupNode(this.rightNeighborId)
    }
  }

  rightSibling () {
    const rightNeighbor = this.rightNeighbor()

    if (rightNeighbor && rightNeighbor.parentId === this.parentId) {
      return rightNeighbor
    }
  }

  childrenCenter (boxW) {
    const first = this.firstChild()
    const last = this.lastChild()
    return (first.prelim + ((last.prelim - first.prelim) + boxW) / 2)
  }

  leftMost (level, depth) {
    if (level >= depth) {
      return this
    }
    if (this.children.length === 0) {
      return
    }

    for (let i = 0, n = this.children.length; i < n; i++) {
      const leftmostDescendant = this.childAt(i).leftMost(level + 1, depth)
      if (leftmostDescendant) {
        return leftmostDescendant
      }
    }
  }
}

export default Tree
