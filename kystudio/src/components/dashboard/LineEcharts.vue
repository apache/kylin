<template>
  <div :id="uuid" :style="style"></div>
</template>

<script>
import * as echarts from 'echarts'

const idGen = () => {
  return new Date().getTime() - 1
}

export default {
  props: {
    height: {
      type: String,
      default: '300px'
    },
    width: {
      type: String,
      default: '450px'
    },

    options: {
      type: Object,
      default: null
    }

  },
  data () {
    return {
      uuid: null,
      myChart: null
    }
  },
  watch: {
    width (a, b) {
      if (this.myChart) {
        setTimeout(() => {
          this.myChart.resize({
            animation: {
              duration: 300
            }
          })
        }, 0)
      }
    },
    options: {
      handler (newValue, oldValue) {
        if (this.myChart) {
          this.myChart.dispose()
          this.myChart = echarts.init(document.getElementById(this.uuid))
          this.myChart.setOption(this.options, {notMerge: true})
        }
      },
      deep: true
    }
  },
  computed: {
    style () {
      return {
        height: this.height,
        width: this.width
      }
    }
  },
  created () {
    this.uuid = idGen()
  },
  mounted () {
    this.myChart = echarts.init(document.getElementById(this.uuid))
    this.myChart.setOption(this.options)
  }
}
</script>
