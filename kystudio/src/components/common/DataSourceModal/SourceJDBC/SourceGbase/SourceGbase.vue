<template>
  <div class="gbase-source">
    <!-- <p class="ksd-mb-24" v-if="connectGbaseSetting.synced">{{$t('alreadySyncTips')}}</p>
    <source-authority-form ref="source-auth-form" :source-type="sourceType" :form="connectGbaseSetting" /> -->
    <source-hive />
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapMutations, mapState } from 'vuex'
import SourceAuthorityForm from '../SourceAuthorityForm/SourceAuthorityForm.vue'
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
    SourceAuthorityForm,
    SourceHive
  },
  computed: {
    ...mapState('DataSourceModal', {
      connectGbaseSetting: state => state.form.connectGbaseSetting
    })
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
