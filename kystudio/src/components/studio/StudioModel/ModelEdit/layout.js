import { modelRenderConfig } from './config'
class Tree {
  constructor (options) {
    this.boxW = modelRenderConfig.tableBoxWidth
    this.boxH = modelRenderConfig.tableBoxHeight
    this.boxML = modelRenderConfig.tableBoxLeft
    this.boxMT = modelRenderConfig.tableBoxTop
    this.tables = options.tables
    this.josnId = 0
    this.rootGuid = options.rootGuid
    this.showLinkCons = options.showLinkCons
    if (!this.rootGuid) {
      throw new Error('rootGuid is required for Tree Layout')
    }
    if (!this.showLinkCons) {
      throw new Error('showLinkCons is required for Tree Layout')
    }
    const rootNode = this.getNodeStructure()
    rootNode.rightNodes = { guid: this.rootGuid, _json_id: 0, children: rootNode.children.filter((n, i) => i % 2 === 0) }
    rootNode.leftNodes = { guid: this.rootGuid, _json_id: 0, children: rootNode.children.filter((n, i) => i % 2 !== 0) }
    this.nodeDB = new NodeDB(rootNode, this)
    this.nodeDB.getRootNode().rightNodes = new NodeDB(rootNode.rightNodes, this, 'rightNodes')
    if (rootNode.leftNodes.children.length > 0) {
      this.nodeDB.getRootNode().leftNodes = new NodeDB(rootNode.leftNodes, this, 'leftNodes')
    }
  }

  getNodeBoundingClientRect (node) {
    const defaultBox = { height: this.boxH, width: this.boxW }
    if (!node) return { nodeHeight: defaultBox.height, nodeWidth: defaultBox.width }
    const nodeGuid = node.guid
    const { height: nodeHeight, width: nodeWidth } = this.tables[nodeGuid]?.drawSize ?? defaultBox
    return { nodeHeight, nodeWidth }
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
    this.firstWalk(rootNode.rightNodes.db[0], 0, 'rightNodes') // rootNode.rightNodes.rightNodeDB[0] 默认右子树的第一个node就是rootNode
    this.secondWalk(rootNode.rightNodes.db[0], 0, 0, 0, 'rightNodes')
    if (rootNode.leftNodes) {
      this.firstWalk(rootNode.leftNodes.db[0], 0, 'leftNodes')
      this.secondWalk(rootNode.leftNodes.db[0], 0, 0, 0, 'leftNodes', true)
      this.thirdWalk(rootNode.leftNodes.db[0], rootNode.rightNodes.db[0], 'leftNodes', true)
    }

    return this
  }

  firstWalk (node, level, nodeType) {
    node.prelim = null
    node.modifier = null
    this.setNeighbors(node, level, nodeType)
    const leftSibling = node.leftSibling()
    const { nodeHeight } = this.getNodeBoundingClientRect(leftSibling)

    if (node.children.length === 0) {
      // set preliminary x-coordinate
      if (leftSibling) {
        node.prelim = leftSibling.prelim + nodeHeight + this.boxMT
      } else {
        node.prelim = 0
      }
    } else {
      // node is not a leaf,  firstWalk for each child
      for (let i = 0; i < node.children.length; i++) {
        this.firstWalk(node.childAt(nodeType, i), level + 1, nodeType)
      }

      const midPoint = node.childrenCenter(nodeType, nodeHeight) - nodeHeight / 2

      if (leftSibling) {
        node.prelim = leftSibling.prelim + nodeHeight + this.boxMT
        node.modifier = node.prelim - midPoint
        this.apportion(node, level, nodeType)
      } else {
        node.prelim = midPoint
      }
    }
    return this
  }

  apportion (node, level, nodeType) {
    let firstChild = node.firstChild(nodeType)
    let firstChildLeftNeighbor = firstChild.leftNeighbor()
    let compareDepth = 1

    while (firstChild && firstChildLeftNeighbor) {
      // calculate the position of the firstChild, according to the position of firstChildLeftNeighbor
      let modifierSumRight = 0
      let modifierSumLeft = 0
      let leftAncestor = firstChildLeftNeighbor
      let rightAncestor = firstChild
      const { nodeHeight } = this.getNodeBoundingClientRect(node)

      for (let i = 0; i < compareDepth; i++) {
        leftAncestor = leftAncestor.parent()
        rightAncestor = rightAncestor.parent()
        modifierSumLeft += leftAncestor.modifier
        modifierSumRight += rightAncestor.modifier
      }

      // find the gap between two trees and apply it to subTrees
      // and mathing smaller gaps to smaller subtrees
      let totalGap = firstChildLeftNeighbor.prelim + modifierSumLeft + nodeHeight + this.boxMT - (firstChild.prelim + modifierSumRight)

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

      firstChild = (firstChild.children.length === 0) ? node.leftMost(0, compareDepth, nodeType) : firstChild.firstChild(nodeType)

      if (firstChild) {
        firstChildLeftNeighbor = firstChild.leftNeighbor()
      }
    }
  }

  secondWalk (node, level, X, Y, nodeType, isLeft) {
    const { nodeWidth } = this.getNodeBoundingClientRect(node)
    const xTmp = X
    const yTmp = node.prelim + Y
    node.X = xTmp
    node.Y = yTmp

    if (node.children.length !== 0) {
      const XX = isLeft ? X - nodeWidth - this.boxML : X + nodeWidth + this.boxML
      this.secondWalk(node.firstChild(nodeType), level + 1, XX, Y + node.modifier, nodeType, isLeft)
    }

    if (node.rightSibling()) {
      this.secondWalk(node.rightSibling(), level, X, Y, nodeType, isLeft)
    }
  }

  thirdWalk (node, targetNode, nodeType, isRoot, modifier) {
    const modifierMove = isRoot ? targetNode.prelim - node.prelim : modifier
    if (modifierMove && isRoot || !isRoot) {
      node.prelim = node.prelim + modifierMove
      node.Y = node.Y + modifierMove
      if (node.children.length !== 0) {
        this.thirdWalk(node.firstChild(nodeType), targetNode, nodeType, false, modifierMove)
      }
      if (node.rightSibling()) {
        this.thirdWalk(node.rightSibling(), targetNode, nodeType, false, modifierMove)
      }
    }
  }

  setNeighbors (node, level, lastNodeOnLevelType) {
    node.leftNeighborId = this[lastNodeOnLevelType + 'OnLevel'][level]
    if (node.leftNeighborId) {
      node.leftNeighbor().rightNeighborId = node.id
    }
    this[lastNodeOnLevelType + 'OnLevel'][level] = node.id
    return this
  }

  resetLevelData () {
    this.rightNodesOnLevel = []
    this.leftNodesOnLevel = []
    return this
  }

  root () {
    return this.nodeDB.getRootNode()
  }
}

class NodeDB {
  constructor (nodeStructure, tree, nodeType) {
    if (nodeType) {
      this.db = []
    } else {
      this.rootNode = null
    }
    const self = this
    const iterateChildren = function (node, parentId) {
      const newNode = self.createNode(node, parentId, tree, nodeType)
      if (node.children && nodeType) {
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

  getChildNode (nodeType, nodeId) {
    return this.rootNode[nodeType].db[nodeId]
  }

  getRootNode () {
    return this.rootNode
  }

  createNode (nodeStructure, parentId, tree, nodeType) {
    const node = new TreeNode(nodeStructure, nodeType ? this.db.length : 0, parentId, tree, nodeType)
    if (!nodeType) {
      this.rootNode = node
    } else {
      this.db.push(node)
    }
    // skip root node (0)
    if (parentId >= 0) {
      const parent = this.get(parentId)
      parent.children.push(node.id)
    }
    return node
  }
}

class TreeNode {
  constructor (nodeStructure, id, parentId, tree, treeType) {
    this.id = id
    this.guid = nodeStructure.guid
    this.parentId = parentId
    this.tree = tree
    this.treeType = treeType
    this.prelim = 0
    this.modifier = 0
    this.leftNeighborId = null
    this.children = []

    return this
  }

  dbGet (nodeType, nodeId) {
    return this.getTreeNodeDb().getChildNode(nodeType, nodeId)
  }

  childAt (nodeType, index) {
    return this.dbGet(nodeType, this.children[index])
  }

  firstChild (nodeType) {
    return this.childAt(nodeType, 0)
  }

  lastChild (nodeType) {
    return this.childAt(nodeType, this.children.length - 1)
  }

  getTreeNodeDb () {
    return this.tree.getNodeDb()
  }

  lookupNode (nodeId) {
    return this.getTreeNodeDb().getChildNode(this.treeType, nodeId)
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

  childrenCenter (nodeType, boxH) {
    const first = this.firstChild(nodeType)
    const last = this.lastChild(nodeType)
    return (first.prelim + ((last.prelim - first.prelim) + boxH) / 2)
  }

  leftMost (level, depth, nodeType) {
    if (level >= depth) {
      return this
    }
    if (this.children.length === 0) {
      return
    }

    for (let i = 0, n = this.children.length; i < n; i++) {
      const leftmostDescendant = this.childAt(nodeType, i).leftMost(level + 1, depth, nodeType)
      if (leftmostDescendant) {
        return leftmostDescendant
      }
    }
  }
}

export default Tree
