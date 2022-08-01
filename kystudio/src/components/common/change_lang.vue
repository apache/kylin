<template>
  <!-- <el-button-group class="switch-button-group change_lang ke-it-lang">
    <el-button size="small" @click="changeLang('en')" :class="{'active':lang=='en'}" class="ke-it-en">EN</el-button>
    <el-button size="small" @click="changeLang('zh-cn')" :class="{'active':lang=='zh-cn'}" class="ke-it-cn">中文</el-button>
  </el-button-group> -->
  <el-dropdown>
  </el-dropdown>
</template>
<script>
  import Vue from 'vue'
  import VueI18n from 'vue-i18n'
  import enLocale from 'kyligence-kylin-ui/lib/locale/lang/en'
  import enKylinLocale from '../../locale/en'
  import { getQueryString } from '../../util/index'
  import { mapState } from 'vuex'
  Vue.use(VueI18n)
  enLocale.kylinLang = enKylinLocale.default
  Vue.locale('en', enLocale)
  import { Component } from 'vue-property-decorator'
  @Component({
    computed: {
      ...mapState({
        messageDirectives: state => state.system.messageDirectives
      })
    },
    methods: {
      changeLang (val) {
        this.lang = 'en'
        this.$store.state.system.lang = 'en'
        Vue.config.lang = this.lang
        Vue.http.headers.common['Accept-Language'] = 'en'
        localStorage.setItem('kystudio_lang', 'en')
        document.documentElement.lang = 'en-us'
      }
    }
  })
  export default class ChangeLang extends Vue {
    data () {
      return {
        defaultLang: 'en',
        lang: localStorage.getItem('kystudio_lang') ? localStorage.getItem('kystudio_lang') : this.defaultLang
      }
    }
    created () {
      // 外链传参数改变语言环境
      var lang = getQueryString('lang')
      if (lang) {
        this.changeLang(lang)
      } else {
        let currentLang = navigator.language
        // 判断除IE外其他浏览器使用语言
        if (!currentLang) {
        // 判断IE浏览器使用语言
          currentLang = navigator.browserLanguage
        }
        if (currentLang && currentLang.indexOf('zh') >= 0) {
          this.defaultLang = 'zh-cn'
        }
        const finalLang = localStorage.getItem('kystudio_lang') ? localStorage.getItem('kystudio_lang') : this.defaultLang
        this.changeLang(finalLang)
      }
      let langMessageList = this.messageDirectives.filter(item => item.action === 'changeLang')
      if (langMessageList.length) {
        this.changeLang(langMessageList.pop().params)
      }
      this.$_bus.$on('changeLang', (val) => {
        if (val !== this.lang) {
          this.changeLang(val)
        }
      })
    }
  }
</script>
<style lang="less">
  @import '../../assets/styles/variables.less';
  .change_lang{
    height: 24px;
    .el-button {
      min-width: 36px;
      border-color: @text-disabled-color;
      &:first-child {
        border-top-left-radius: 2px;
        border-bottom-left-radius: 2px;
      }
      &:last-child {
        border-top-right-radius: 2px;
        border-bottom-right-radius: 2px;
      }
      &.active {
        background: @line-border-color;
        box-shadow: inset 1px 1px 2px 0 @grey-1;
      }
    }
  }
</style>
