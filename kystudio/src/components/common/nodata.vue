<template>
	<div class="no-data">
	    {{tips}}
	</div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'

@Component({
  props: {
    content: {
      type: String,
      default: ''
    }
  }
})
export default class NoData extends Vue {
  get tips () {
    return this.content || this.$t('kylinLang.common.noData')
  }
}

</script>

<style scoped="" lang="less">
@import "../../assets/styles/variables.less";
div{
 pointer-events:none;
 text-align: center;
 color:@text-disabled-color;
 position: absolute;
 top: 30%;
 text-align: center;
 width: 100%;
}
</style>
