<template>
  <div :id="uuid" :style="style"></div>
</template>

<script>
import * as echarts from 'echarts'
import {Component, Watch} from 'vue-property-decorator'
import Vue from 'vue'

const idGen = () => {
  return new Date().getTime() - 1
}

@Component({
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
  }
})

export default class LineEcharts extends Vue {
  @Watch('width')
  onWithChange () {
    if (this.myChart) {
      setTimeout(() => {
        this.myChart.resize({
          animation: {
            duration: 300
          }
        })
      }, 0)
    }
  }

  @Watch('options', {deep: true})
  onOptionChange () {
    if (this.myChart) {
      this.myChart.dispose()
      this.myChart = echarts.init(document.getElementById(this.uuid))
      this.myChart.setOption(this.options, {notMerge: true})
    }
  }

  get style () {
    return {
      height: this.height,
      width: this.width
    }
  }

  created () {
    this.uuid = idGen()
  }

  mounted () {
    this.myChart = echarts.init(document.getElementById(this.uuid))
    this.myChart.setOption(this.options)
  }
}
</script>
