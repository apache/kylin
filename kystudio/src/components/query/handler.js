export function getOptions (that) {
  if (that.charts.type === 'lineChart') {
    return initLineChart(that)
  } else if (that.charts.type === 'barChart') {
    return initBarChart(that)
  } else if (that.charts.type === 'pieChart') {
    return initPieChart(that)
  }
}

// 折线图
function initLineChart (that) {
  const dataMap = combinationData(that)
  const yMap = Object.values(dataMap)
  // 折线图按照 x 轴排序
  const dataList = Object.keys(dataMap).map((item, index) => ({key: item, value: yMap[index], tm: new Date(item).getTime()}))
  const sortList = dataList.sort((a, b) => a.tm - b.tm)
  const xData = sortList.map(it => it.key)
  const yData = sortList.map(it => it.value)
  return {
    color: ['#3398DB'],
    xAxis: {
      type: 'category',
      data: xData,
      nameLocation: 'start'
    },
    yAxis: {
      type: 'value'
    },
    series: [{
      data: yData,
      type: 'line'
    }],
    tooltip: {},
    dataZoom: [
      {
        show: true,
        realtime: true,
        start: 30,
        end: 70
      }
    ]
  }
}

// 柱状图
function initBarChart (that) {
  const dataMap = combinationData(that)
  const yMap = Object.values(dataMap)
  // 柱状图需要按照 Y 轴排序
  const dataList = Object.keys(dataMap).map((item, index) => ({key: item, value: yMap[index]}))
  const sortList = dataList.sort((a, b) => a.value - b.value)
  const xData = sortList.map(it => it.key)
  const yData = sortList.map(it => it.value)
  return {
    color: ['#3398DB'],
    xAxis: [{
      type: 'category',
      data: xData,
      nameLocation: 'start'
    }],
    yAxis: [{
      type: 'value'
    }],
    series: [{
      type: 'bar',
      barWidth: '60%',
      data: yData
    }],
    tooltip: {},
    dataZoom: [
      {
        show: true,
        realtime: true,
        start: 30,
        end: 70
      }
    ]
  }
}

// 饼图
function initPieChart (that) {
  const dataMap = combinationData(that)
  const xData = Object.keys(dataMap).slice(0, 1000)
  const yData = Object.values(dataMap).slice(0, 1000)
  const pieList = []
  xData.forEach((item, index) => {
    pieList.push({name: item, value: yData[index]})
  })
  return {
    series: [{
      type: 'pie',
      data: pieList,
      emphasis: {
        labelLine: {
          show: xData.length <= 50
        }
      },
      label: {
        show: xData.length <= 50
      }
    }],
    tooltip: {
      trigger: 'item'
    }
  }
}

function combinationData (that) {
  if (that.extraoption.results) {
    const { dimension, measure } = that.charts
    const dataMap = {}
    const dimensionIndex = that.tableMetaBackup.findIndex(item => item.name === dimension)
    const measureIndex = that.tableMetaBackup.findIndex(item => item.name === measure)
    const xSourceData = that.extraoption.results.map(it => it[dimensionIndex])
    const ySourceData = that.extraoption.results.map(it => it[measureIndex])
    xSourceData.forEach((item, index) => {
      if (item in dataMap) {
        dataMap[item] = +dataMap[item] + (+ySourceData[index])
      } else {
        dataMap[item] = +ySourceData[index]
      }
    })
    return dataMap
  } else {
    return {}
  }
}

export function compareDataSize (that) {
  const data = combinationData(that)
  return { xData: Object.keys(data), yData: Object.values(data) }
}
