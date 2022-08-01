<template>
	<span class="tip_box">
    <!-- 通过props tips传入纯文本 -->
		<el-tooltip :content="tips" popper-class="common-tip-layout" :placement="placement||'top'" :disabled="visible" :open-delay="tooltipDelayTime">
      <!-- 通过slot传入带dom或事件的内容 -->
      <div slot="content"><slot name="content"></slot></div>
      <!-- 通过props传入带dom标签的内容 -->
      <div slot="content" v-html="content" v-if="content"></div>
		  <span class="icon"><slot></slot></span>
		</el-tooltip>
	</span>
</template>
<script>
  import { tooltipDelayTime } from '../../config/index'
  import Vue from 'vue'
  import { Component } from 'vue-property-decorator'
  @Component({
    props: ['tips', 'content', 'trigger', 'placement', 'disabled'],
    watch: {
      disabled (val) {
        this.visible = val
      }
    }
  })
  export default class CommonTip extends Vue {
    data () {
      return {
        tooltipDelayTime: tooltipDelayTime,
        visible: this.disabled
      }
    }
  }
</script>
<style lang="less">
.tip_box{
  .icon{
    font-size: 14px;
  }
}
.common-tip-layout.el-tooltip__popper {
  .popper__arrow {
    &::after {
      content: none\0;
    }
  }
}
</style>
