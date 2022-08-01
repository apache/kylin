<template>
  <div class="model-json" v-loading="isLoading">
    <el-input
      class="model-json_input"
      :value="JSON.stringify(jsonInfo, '', 4)"
      type="textarea"
      :rows="18"
      :readonly="true">
    </el-input></div>
</template>
<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { handleSuccess, handleError } from 'util/index'
import { mapActions, mapGetters } from 'vuex'
@Component({
  props: ['model'],
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ])
  },
  methods: {
    ...mapActions({
      getModelJson: 'GET_MODEL_JSON'
    })
  }
})
export default class modelJSON extends Vue {
  jsonInfo = ''
  isLoading = false
  mounted () {
    this.isLoading = true
    this.getModelJson({
      model: this.model,
      project: this.currentSelectedProject
    }).then((res) => {
      handleSuccess(res, (data) => {
        this.jsonInfo = JSON.parse(data)
        this.isLoading = false
      })
    }, (res) => {
      handleError(res)
      this.isLoading = false
    })
  }
}
</script>
<style lang="less">
  @import '../../../../../assets/styles/variables.less';
  .model-json {
    height: calc(~'100% - 50px');
    .model-json_input {
      height: 100%;
      .el-textarea__inner {
        height: 100%;
      }
      .el-textarea__inner:focus { 
        border-color: @line-border-color;
      }
    }
  }
</style>
