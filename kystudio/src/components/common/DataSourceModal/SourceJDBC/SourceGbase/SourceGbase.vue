<template>
  <div class="gbase-source">
    <source-hive />
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapMutations } from 'vuex'
import SourceHive from '../../SourceHive/SourceHive.vue'
import locales from './locales'
import { types } from '../../store'

@Component({
  props: {
    sourceType: {
      type: Number
    }
  },
  components: {
    SourceHive
  },
  methods: {
    ...mapMutations('DataSourceModal', {
      updateJDBCConfig: types.UPDATE_JDBC_CONFIG
    })
  },
  locales
})
export default class SourceGbase extends Vue {
  created () {
    this.updateJDBCConfig(this.sourceType)
  }
}
</script>

<style lang="less">
.gbase-source {
  padding: 24px;
  box-sizing: border-box;
}
</style>
