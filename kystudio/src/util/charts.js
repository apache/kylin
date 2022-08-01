import filterElements from '../filter/index'

export default {
  // 折线图标options
  line: (_this, xData, yData, sourceData) => {
    return {
      grid: {
        top: 30,
        left: 70,
        right: 30,
        bottom: 30
      },
      xAxis: {
        type: 'category',
        boundaryGap: false,
        min: 'dataMin',
        max: 'dataMax',
        data: xData || [],
        axisLabel: {
          interval: 'auto',
          align: 'center',
          fontSize: 10
        }
      },
      yAxis: {
        type: 'value',
        axisLabel: {
          formatter: '{value} T'
        }
      },
      color: ['#15BDF1'],
      series: [{
        data: yData || [],
        type: 'line',
        areaStyle: {
          color: {
            type: 'linear',
            x: 0,
            y: 0,
            x2: 0,
            y2: 1,
            colorStops: [{
              offset: 0, color: '#15BDF1' // color at 0% position
            }, {
              offset: 1, color: '#fff' // color at 100% position
            }],
            global: false // false by default
          }
        },
        itemStyle: {
          normal: {
            color: '#15BDF1'
          }
        },
        symbol: 'circle'
      }],
      tooltip: {
        show: true,
        backgroundColor: '#fff',
        extraCssText: 'box-shadow:0px 0px 6px 0px rgba(229,229,229,1),0px 2px 4px 0px rgba(229,229,229,1);',
        textStyle: {
          color: '#323232'
        },
        axisPointer: {
          type: 'line',
          label: {
            show: false
          }
        },
        padding: [5, 10],
        position: 'bottom',
        formatter: (params) => {
          return `<span>${_this.$t('usedCapacity')}：${filterElements.dataSize(sourceData[Object.keys(sourceData).sort((a, b) => a - b)[params.dataIndex]])} </span>`
        }
      }
    }
  },
  treeMap: function (_this, data, formatUtil, options = {}) {
    return {
      grid: {
        width: '100%',
        height: '100%'
      },
      tooltip: {
        formatter: (info) => {
          let capacity = info.data.capacity
          let treePathInfo = info.treePathInfo
          let treePath = []

          for (let i = 1; i < treePathInfo.length; i++) {
            treePath.push(treePathInfo[i].name)
          }
          return [
            '<div class="tooltip-title">' + _this.$t('projectNameByTreeMap') + formatUtil.encodeHTML(treePath.join('/')) + '</div>',
            _this.$t('usedCapacityByTreeMap') + capacity
          ].join('')
        }
      },
      series: [
        {
          type: 'treemap',
          // visibleMin: 500,
          width: '100%',
          height: '100%',
          top: 0,
          left: 0,
          label: {
            show: true,
            formatter: '{b}',
            position: [10, 10]
          },
          itemStyle: {
            borderColor: '#fff'
          },
          breadcrumb: {
            show: false
          },
          tooltip: {
            backgroundColor: '#fff',
            textStyle: {
              color: '#323232'
            },
            extraCssText: 'box-shadow:0px 0px 6px 0px rgba(229,229,229,1),0px 2px 4px 0px rgba(229,229,229,1);'
          },
          roam: false,
          nodeClick: false,
          levels: [
            {
              colorSaturation: [0.85, 0.65],
              itemStyle: {
                color: '#3AA0E5',
                borderWidth: 0,
                gapWidth: 3
              }
            },
            {
              itemStyle: {
                gapWidth: 1
              }
            },
            {
              colorSaturation: [0.85, 0.65],
              itemStyle: {
                color: '#3AA0E5',
                gapWidth: 1,
                borderColorSaturation: 0.6
              }
            }
          ],
          data: data
        }
      ],
      ...options
    }
  }
}
