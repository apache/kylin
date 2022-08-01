<template>
  <div>
    <div class="shade-box"></div>
    <div class="global-loading-box"><img src="../../assets/img/kylin-loading.gif"></div>
  </div>
</template>
<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
@Component({})
export default class GlobaleLoading extends Vue {
  data () {
    return {
      visible: false
    }
  }
}
</script>
<style lang="less">
  @import '../../assets/styles/variables.less';
  .shade-box {
    position: absolute;
    top:0;
    width: 100%;
    bottom:0;
    z-index:99999;
    background-color: @fff;
    opacity: 0.65;
  }
  .global-loading-box{
    z-index:999999;
    position: absolute;
    background-color: @fff;
    width:320px;
    height: 200px;
    text-align: center;
    left:50%;
    margin-left: -160px;
    margin-top: -100px;
    top:50%;
    border:solid 1px @text-placeholder-color;
    img{
      height: 56px;
      top:50%;
      margin-top: -28px;
      position: relative;
    }
  }
</style>
