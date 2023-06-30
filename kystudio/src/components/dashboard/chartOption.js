export default {
  barChartOptions: (xData, yData) => {
    return {
      title: {
        text: '',
        left: 'center',
        top: -5
      },
      height: 255,
      grid: {
        top: 25,
        left: 45,
        right: 15,
        bottom: 0
      },
      tooltip: {
        trigger: 'axis',
        axisPointer: {
          type: 'shadow'
        }
      },
      xAxis: [
        {
          type: 'category',
          data: xData || [],
          axisTick: {
            alignWithLabel: true
          }
        }
      ],
      yAxis: [
        {
          type: 'value',
          axisTick: false
        }
      ],
      series: [
        {
          name: 'value',
          type: 'bar',
          barWidth: '60%',
          data: yData || [],
          label: {
            show: false
          },
          itemStyle: {
            normal: {
              color: function (params) {
                const colorList = ['#5470c6', '#91cc75', '#fac858', '#ee6666', '#73c0de', '#3ba272', '#fc8452', '#9a60b4', '#ea7ccc']
                return colorList[params.dataIndex]
              }
            }
          }
        }
      ]
    }
  },
  lineChartOptions: (xData, yData) => {
    return {
      title: {
        text: '',
        left: 'center',
        top: 15
      },
      grid: {
        top: 45,
        left: 45,
        right: 15,
        height: 235
      },
      tooltip: {
        trigger: 'axis'
      },
      xAxis: [
        {
          type: 'category',
          data: xData || [],
          axisTick: {
            alignWithLabel: true
          }
        }
      ],
      yAxis: [
        {
          type: 'value',
          axisTick: false
        }
      ],
      series: [
        {
          name: 'value',
          type: 'line',
          color: '#5470c6',
          data: yData || []
        }
      ]
    }
  }
}
