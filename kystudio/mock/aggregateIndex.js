let nodeTotalCount = 0

function getAggregateData () {
  const data =  {
    cuboidDesc: {
      id: Math.random() * 100000000,
      dimensions: [
        "KYLIN_ACCOUNT.ACCOUNT_ID",
        "KYLIN_SALES.SELLER_ID"
      ],
      size: Math.random() * 200000,
      range: {
        startDate: new Date('Thu Jan 1 2018 00:00:00 GMT+0800 (中国标准时间)').setDate(nodeTotalCount),
        endDate: new Date('Thu Jan 1 2018 00:00:00 GMT+0800 (中国标准时间)').setDate(nodeTotalCount + 1)
      },
      savedQueryCount: Math.random() * 100,
      measures: [],
      layouts: []
    },
    children: [],
    parent: []
  }
  nodeTotalCount++

  return data
}

function getAggregateTree () {
  const root = getAggregateData()
  getAggregateNode(root)
  return root
}

function getAggregateNode (parent) {
  const nodeCount = Math.round(Math.random() * 3)
  for (let i = 0; i < nodeCount; i++) {
    parent.children.push(getAggregateData())

    if (nodeTotalCount < 99) {
      getAggregateNode(parent.children[i])
    }
  }
}

export const aggregateTree = {
  "data": {
    "code": "000",
    "data": {
      "size": 120,
      "data": getAggregateTree()
    },
    "msg": ""
  }
}
