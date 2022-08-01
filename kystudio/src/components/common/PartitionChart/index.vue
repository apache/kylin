<template>
  <div class="partition-chart">
    <!-- <div class="operator">
      <label><input type="radio" name="mode" value="size">Size</label>
      <label><input type="radio" name="mode" value="count" checked>Count</label>
    </div> -->
    <svg ref="svg" v-if="isChartCreated" :width="elementWidth" :height="elementHeight">
      <defs>
        <filter id="shadow" x="-50%" y="-50%" width="200%" height="200%">
          <feOffset result="offOut" in="SourceAlpha" dx="0" dy="0" />
          <feGaussianBlur result="blurOut" in="offOut" stdDeviation="2" />
          <feBlend in="SourceGraphic" in2="blurOut" mode="normal" />
        </filter>
        <pattern
          v-for="(background, backgroundName) in backgroundMaps"
          :key="backgroundName"
          :id="backgroundName"
          patternUnits="userSpaceOnUse"
          :width="background.width"
          :height="background.height">
          <image
            x="0"
            y="0"
            :xlink:href="background.url"
            :width="background.width"
            :height="background.height" />
        </pattern>
      </defs>
    </svg>
    <div class="tip" v-if="tip !== ''" :style="{ left: `${tipX + 20}px`, top: `${tipY + 48}px` }">{{tip}}</div>
  </div>
</template>

<script>
import Vue from 'vue'
import * as d3 from 'd3'
import { Component, Watch } from 'vue-property-decorator'

@Component({
  props: ['data', 'searchId', 'backgroundMaps']
})
export default class PartitionChart extends Vue {
  isChartCreated = false
  elementWidth = 0
  elementHeight = 0
  tip = ''
  tipX = 0
  tipY = 0
  baseColor = [
    '#0372EA', '#28741D', '#5F2BE7', '#CC7E04', '#0F7A8D', '#BA0909'
  ]
  @Watch('searchId')
  async onSearchIdChange (searchId) {
    this.d3data.path.each(function (d) {
      if (d.cuboid && String(d.cuboid.id) === searchId) {
        this.dispatchEvent(new Event('click'))
      }
    })
  }
  @Watch('data')
  async onDataChange () {
    if (this.data[0]) {
      await this.cleanSVG()
      await this.drawSVG()
    }
  }
  async mounted () {
    if (this.data[0]) {
      await this.cleanSVG()
      await this.drawSVG()
    }
  }
  cleanSVG () {
    return new Promise(resolve => {
      this.isChartCreated = false
      setTimeout(() => {
        this.isChartCreated = true
        resolve()
      }, 20)
    })
  }
  drawSVG () {
    return new Promise(resolve => {
      this.d3data = {}
      this.elementWidth = this.$el.clientWidth
      this.elementHeight = this.$el.clientHeight

      setTimeout(() => {
        this.initChart()
        resolve()
      }, 20)
    })
  }
  updateSVG () {
    this.d3data.path
      .attr('filter', function (d) { return d.isHover || d.isSelected ? 'url(#shadow)' : null })
      .style('stroke', function (d) { return d.isSelected ? '#0E9BFB' : '#FFFFFF' })

    this.d3data.path
      .filter(function (d) { return d.isSelected })
      .each(function () { this.parentNode.appendChild(this) })
    this.d3data.path
      .filter(function (d) { return d.isHover })
      .each(function () { this.parentNode.appendChild(this) })
  }
  initChart () {
    const self = this
    const width = this.elementWidth
    const height = this.elementHeight - 30
    const radius = Math.min(width, height) / 2
    // let isShowDefaultChart = false

    const svg = d3.select(this.$refs['svg'])
      .append('g')
      .attr('transform', 'translate(' + width / 2 + ',' + height * 0.52 + ')')

    const partition = d3.layout.partition()
      .sort(null)
      .size([2 * Math.PI, radius * radius])
      .value(function (d) { return 1 })

    const arc = d3.svg.arc()
      .startAngle(function (d) { return d.x })
      .endAngle(function (d) { return d.x + d.dx })
      .innerRadius(function (d) { return Math.sqrt(d.y) })
      .outerRadius(function (d) { return Math.sqrt(d.y + d.dy) })

    const path = this.d3data.path = svg.datum(this.data[0]).selectAll('path')
      .data(partition.nodes)
      .enter().append('path')
      .attr('display', function (d) { return d.depth ? null : 'none' }) // hide inner ring
      .attr('d', arc)
      .style('stroke', '#fff')
      .style('fill-rule', 'evenodd')
      .each(stash)

    path.on('mouseenter', function (d) {
      d.isHover = true
      self.updateSVG()
    })
    path.on('mouseout', function (d) {
      d.isHover = false
      self.tip = ''
      self.updateSVG()
    })
    path.on('click', function (d) {
      path.each(function (d) { d.isSelected = false })
      d.isSelected = true
      self.updateSVG()
      self.$emit('on-click-node', d)
    })
    path.each(function (d) {
      // if (!isShowDefaultChart && d3.select(this).attr('display') === null) {
      //   this.dispatchEvent(new Event('click'))
      //   isShowDefaultChart = true
      // }
      this.addEventListener('mousemove', (event) => {
        self.tipX = event.offsetX
        self.tipY = event.offsetY
        self.tip = Vue.filter('dataSize')(d.cuboid.storage_size)
      })
    })

    d3.select(this.$el).selectAll('input').on('change', function change () {
      const value = this.value === 'count'
        ? function () { return 1 }
        : function (d) { return d.size }

      path
        .data(partition.value(value).nodes)
        .transition()
        .duration(1500)
        .attrTween('d', arcTween)
    })

    // Stash the old values for transition.
    function stash (d) {
      d.x0 = d.x
      d.dx0 = d.dx
    }

    // Interpolate the arcs in data space.
    function arcTween (a) {
      const i = d3.interpolate({x: a.x0, dx: a.dx0}, a)
      return function (t) {
        const b = i(t)
        a.x0 = b.x
        a.dx0 = b.dx
        return arc(b)
      }
    }

    d3.select(self.frameElement).style('height', height + 'px')
    this.updatePathColor(this.data[0])
  }
  updatePathColor (data) {
    const { children = [] } = data
    const _this = this
    const startLight = 30
    children.forEach((child, index) => {
      const colorIndex = index % this.baseColor.length
      child.color = this.baseColor[colorIndex]
      if (child.children) {
        child.children.forEach(updateChildrenColor)
      }
    })

    this.d3data.path.each(function (d) {
      d3.select(this).style('fill', function (d) {
        return _this.backgroundMaps[d.background] ? `url(#${d.background})` : d.color
      })
    })
    function updateChildrenColor (child, index) {
      const lightStep = (100 - startLight) / child.maxLevel
      const lightness = lightStep * child.depth + startLight
      const parent = child.parent
      const hcl = d3.hcl(parent.color)
      const parentHue = hcl.h
      const parentChroma = hcl.c
      const parentLightness = hcl.l
      const currentLightness = (lightness - parentLightness) * Math.random() + parentLightness

      child.color = d3.hcl(parentHue, parentChroma, currentLightness > 100 ? 100 : currentLightness).toString()
      if (child.children) {
        child.children.forEach(updateChildrenColor)
      }
    }
    // for (let i = 0; i < 1000; i += 10) {
    //   const color = d3.hcl(i, 100, 50)
    //   console.log('%c%s', `color:${color.toString()};`, String(i))
    // }
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';

.partition-chart {
  width: calc(~'100% - 20px');
  height: 440px;
  .operator {
    position: absolute;
    right: 0;
    top: 50px;
    label {
      margin-right: 10px;
    }
    input {
      margin-right: 10px;
    }
  }
  .tip {
    position: absolute;
    transform: translate(-50%, -100%);
    padding: 10px;
    background: @text-normal-color;
    color: #fff;
    pointer-events: none;
  }
}
</style>
