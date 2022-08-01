 <template>
 <div class="area_label" :class="changeable">
  <el-select ref="select"
    v-model="selectedL"
    @remove-tag="removeTag"
    @change="change"
    style="width:100%"
    :disabled='disabled'
    multiple
    filterable
    remote
    size="medium"
    :remote-method="remoteMethodSync"
    :allow-create='allowcreate'
    :popper-class="changeable"
    :placeholder="placeholder"
    :duplicate-remove="duplicateremove">
    <el-option-group v-if="selectGroupOne.length">
      <el-option v-for="item in selectGroupOne" :key="item.value" :label="item.label" :value="item.value"></el-option>
    </el-option-group>
    <el-option-group>
      <el-option
        v-for="(item, index) in baseLabel"
        :key="index"
        :label="item.label"
        :value="item.value" >
      </el-option>
    </el-option-group>
  </el-select>
  </div>
</template>
<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
@Component({
  props: {
    labels: Array,
    refreshInfo: [String, Array, Boolean, Number, Object],
    selectedlabels: Array,
    isNeedNotUpperCase: Boolean,
    placeholder: String,
    changeable: String,
    datamap: Object,
    disabled: Boolean,
    allowcreate: Boolean,
    ignoreSplitChar: Boolean,
    validateRegex: RegExp,
    errorValues: Array,
    splitChar: String,
    duplicateremove: Boolean,
    validateFailedMove: {
      type: Boolean,
      default: true
    },
    isSignSameValue: Boolean,
    isCache: Boolean,
    remoteMethod: Function,
    selectGroupOne: {
      type: Array,
      default () {
        return []
      }
    },
    remoteSearch: {
      type: Boolean,
      default: true
    }
  },
  computed: {
    'baseLabel' () {
      var arr = []
      var len = this.labels && this.labels.length || 0
      for (var k = 0; k < len; k++) {
        if (this.labels[k]) {
          var obj = {
            label: (this.datamap && this.datamap.label) ? this.labels[k][this.datamap.label] : this.labels[k],
            value: (this.datamap && this.datamap.value) ? this.labels[k][this.datamap.value] : this.labels[k]
          }
          arr.push(obj)
        }
      }
      this.$nextTick(() => {
        this.tags = Array.prototype.slice.call(this.$el.querySelectorAll('.el-tag'))
      })
      return arr
    }
  },
  watch: {
    selectedlabels (val) {
      this.selectedL = val.map((item) => {
        return this.isNeedNotUpperCase ? item : item.toLocaleUpperCase()
      })
    },
    // 保证组件在外部切换校验类型的时候能够动态切换校验表达式
    validateRegex (val) {
      this.validateReg = val
    },
    errorValues (val) {
      this.signServerValidateFailedTags()
    }
  },
  methods: {
    remoteMethodSync (query) {
      if (this.remoteSearch && this.remoteMethod) {
        this.query = query
        this.remoteMethod(query)
      } else {
        this.query = query
      }
    },
    change (e) {
      this.$nextTick(() => {
        this.$emit('change')
        this.tags = Array.prototype.slice.call(this.$el.querySelectorAll('.el-tag'))
        if (this.allowcreate && e.length > 0) {
          let item = this.isNeedNotUpperCase ? e[e.length - 1] : (e[e.length - 1]).toLocaleUpperCase()
          let result = this.filterCreateTag(item)
          var splitChar = this.splitChar || ';'
          var regOfSeparate = new RegExp(splitChar)
          if (!this.ignoreSplitChar && regOfSeparate.test(item)) {
            if (result && result.length > 0) {
              this.selectedL.splice(this.selectedL.length - 1, 1)
              this.selectedL = this.selectedL.concat(result)
            }
          }
          if (result && result.length <= 0) {
            this.selectedL.splice(this.selectedL.length - 1, 1)
          }
        }
        // 都转为大写
        let temp = this.selectedL.map((item) => {
          return this.isNeedNotUpperCase ? item : item.toLocaleUpperCase()
        })
        // 去重返回
        this.duplicateremove && (this.selectedL = [...new Set(temp)])
        this.$emit('refreshData', this.selectedL, this.refreshInfo)
        this.bindTagClick()
        this.resetErrortags()
        this.isSignSameValue && this.signSameTags()
        !this.validateFailedMove && this.signValidateFailedTags()
        this.errorValues && this.errorValues.length > 0 && this.signServerValidateFailedTags()
      })
    },
    bindTagClick () {
      this.tags = Array.prototype.slice.call(this.$el.querySelectorAll('.el-tag'))
      var arealabel = this.$el.querySelectorAll('.el-select__tags span')
      if (arealabel.length) {
        arealabel[0].onclick = (e) => {
          var ev = e || window.event
          var target = ev.target || ev.srcElement
          if (target && (target.className.indexOf('el-tag') >= 0 || target.className.indexOf('el-select__tags') || target.className.indexOf('el-select__tags-text') >= 0)) {
            if (e.stopPropagation) {
              e.stopPropagation()
            } else {
              window.event.cancelBubble = true
            }
            this.selectTag(ev)
          }
        }
      }
    },
    resetErrortags () {
      const tags = Array.prototype.slice.call(this.$el.querySelectorAll('.el-tag'))
      tags.forEach((t, index) => {
        let tagClassName = tags[index].className
        if (tags[index] && tagClassName.indexOf('error-tag') > -1) {
          tags[index].className = tagClassName.replace(/error-tag/g, '')
        }
      })
    },
    // 标记重复的tag
    signSameTags () {
      setTimeout(() => {
        let indexes = []
        const tags = Array.prototype.slice.call(this.$el.querySelectorAll('.el-tag'))
        const tagText = tags.map(item => item.querySelector('.el-select__tags-text') && item.querySelector('.el-select__tags-text').innerText || item.querySelector('.el-select__tags-text').textContent)
        tagText.map(it => it.trim()).forEach((element, index, self) => {
          if (self.indexOf(element.trim()) !== self.lastIndexOf(element.trim())) {
            tags[index] && (tags[index].className += ' error-tag')
            indexes.push(index)
          }
        })
        this.$emit('duplicateTags', indexes.length > 0)
      }, 200)
    },
    // 标记校验失败的tag
    signValidateFailedTags () {
      setTimeout(() => {
        let indexes = []
        const tags = Array.prototype.slice.call(this.$el.querySelectorAll('.el-tag'))
        const tagText = tags.map(item => item.querySelector('.el-select__tags-text') && item.querySelector('.el-select__tags-text').innerText)
        var regExp = new RegExp(this.validateReg)
        tagText.forEach((item, index) => {
          if (!regExp.test(item.trim())) {
            tags[index] && (tags[index].className += ' error-tag')
            indexes.push(index)
          } else if (this.isCache) {
            let cacheTags = JSON.parse(localStorage.getItem('cacheTags'))
            if (!cacheTags) {
              localStorage.setItem('cacheTags', JSON.stringify([item.trim()]))
            } else if (cacheTags.indexOf(item.trim()) === -1) {
              cacheTags.unshift(item.trim())
              cacheTags.length > 5 ? localStorage.setItem('cacheTags', JSON.stringify(cacheTags.slice(0, 5))) : localStorage.setItem('cacheTags', JSON.stringify(cacheTags))
            }
          }
          if (this.errorValues && this.errorValues.indexOf(item.trim()) !== -1) {
            tags[index] && (tags[index].className += ' error-tag')
          }
        })
        this.$emit('validateFailedTags', indexes.length > 0)
      }, 200)
    },
    // 标记后端校验失败的tag
    signServerValidateFailedTags () {
      setTimeout(() => {
        const tags = Array.prototype.slice.call(this.$el.querySelectorAll('.el-tag'))
        const tagText = tags.map(item => item.querySelector('.el-select__tags-text') && item.querySelector('.el-select__tags-text').innerText)
        tagText.forEach((item, index) => {
          if (this.errorValues && this.errorValues.indexOf(item.trim()) !== -1) {
            tags[index] && (tags[index].className += ' error-tag')
          }
        })
      }, 200)
    },
    removeTag (data) {
      // var len = this.selectedL && this.selectedL.length || 0
      // for (var k = 0; k < len; k++) {
      //   if (this.selectedL[k] === data) {
      //     this.selectedL.splice(k, 1)
      //     break
      //   }
      // }
      this.resetErrortags()
      this.isSignSameValue && this.signSameTags()
      !this.validateFailedMove && this.signValidateFailedTags()
      this.errorValues && this.errorValues.length > 0 && this.signServerValidateFailedTags()
      this.$emit('removeTag', data, this.refreshInfo)
    },
    selectTag (e) {
      var ev = e || window.event
      var target = ev.target || ev.srcElement
      if (target && (target.className.indexOf('el-tag') >= 0 || target.className.indexOf('el-select__tags') || target.className.indexOf('el-select__tags-text') >= 0)) {
        this.$emit('checklabel', target.innerText, target)
      }
    },
    filterCreateTag (item) {
      if (!item) {
        return []
      }
      if (this.validateReg) {
        var regExp = new RegExp(this.validateReg)
        if (!regExp.test(item)) {
          this.$emit('validateFail')
          if (this.validateFailedMove) {
            return []
          }
        }
      }
      // 忽略分隔符
      if (this.ignoreSplitChar) {
        return [item]
      }
      var result = []
      // 分隔符
      var splitChar = this.splitChar || ';'
      var regOfSeparate = new RegExp(splitChar)
      if (item && regOfSeparate.test(item)) {
        Array.prototype.push.apply(result, item.split(regOfSeparate))
      } else if (item) {
        result.push(item)
      }
      result = result.map((item) => {
        return item.replace(/^\s+|\s+$/g, '')
      })
      // result = result.filter((item) => {
      //   return item
      // })
      return result
    },
    clearDuplicateValue () {
      this.selectedL = [...new Set(this.selectedL)]
      this.$emit('refreshData', this.selectedL, this.refreshInfo)
      this.resetErrortags()
      this.signSameTags()
      this.isSignSameValue && this.signSameTags()
      !this.validateFailedMove && this.signValidateFailedTags()
      this.errorValues && this.errorValues.length > 0 && this.signServerValidateFailedTags()
    },
    manualInputEvent () {
      // 处理单独录入的情况 start
      if (this.allowcreate && this.query) {
        let query = this.isNeedNotUpperCase ? this.query : this.query.toLocaleUpperCase()
        var result = this.filterCreateTag(query)
        if (result && result.length > 0) {
          this.selectedL = this.selectedL.concat(result)
          // 都转为大写
          let temp = this.selectedL.map((item) => {
            return this.isNeedNotUpperCase ? item : item.toLocaleUpperCase()
          })
          // 去重返回
          this.duplicateremove && (this.selectedL = [...new Set(temp)])
          this.resetErrortags()
          this.isSignSameValue && (this.selectedL = this.selectedL.filter(it => it), this.signSameTags())
          !this.validateFailedMove && (this.selectedL = this.selectedL.filter(it => it), this.signValidateFailedTags())
          this.errorValues && this.errorValues.length > 0 && (this.selectedL = this.selectedL.filter(it => it), this.signServerValidateFailedTags())
          this.$emit('refreshData', this.selectedL, this.refreshInfo)
        }
        if (this.$refs.select.$refs.input) {
          this.$refs.select.$refs.input.value = ''
          this.$refs.select.$refs.input.click()
          setTimeout(() => {
            this.query = ''
            this.$refs.select.$refs.input.focus()
          }, 0)
        }
      }
      // 处理单独录入的情况end
    }
  }
})
export default class LabelArea extends Vue {
  data () {
    return {
      selectedL: this.selectedlabels,
      tags: [],
      query: '',
      validateReg: this.validateRegex
    }
  }

  mounted () {
    if (this.allowcreate) {
      this.$refs.select.$refs.input.onkeydown = (ev) => {
        ev = ev || window.event
        if (ev.keyCode !== 13) {
          return
        }
        this.manualInputEvent()
      }
      this.$refs.select.$refs.input.onblur = () => {
        if (this.query) {
          this.manualInputEvent()
        }
        this.resetErrortags()
        this.isSignSameValue && this.signSameTags()
        !this.validateFailedMove && this.signValidateFailedTags()
        this.errorValues && this.errorValues.length > 0 && this.signServerValidateFailedTags()
      }
    }
    this.bindTagClick()
    this.$nextTick(() => {
      this.tags = Array.prototype.slice.call(this.$el.querySelectorAll('.el-tag'))
      this.$emit('loadComplete', this.refreshInfo)
    })
  }
}
</script>
<style lang="less">
@import '../../assets/styles/variables.less';
.unchange {
    display:none;
}
.area_label {
  overflow: hidden;
}
.area_label.unchange {
  display: block;
}
.unchange{
  .el-select-dropdown__empty {
     display:none;
  }
  .el-tag{
    cursor: pointer;
  }
  .el-select .el-input {
    .el-input__icon {
      display:none
    }
  }

}
.area_label {
  // .el-tag__close{
  //   position: absolute;
  //   right: 0;
  //   top: 2px;
  // }
  // .el-tag{
  //   max-width:100%;
  //   overflow:hidden;
  //   position: relative;
  //   padding-right: 20px;
  //   float: left;
  //   .el-select__tags-text{
  //     display: block;
  //     height: 22px;
  //     line-height: 22px;
  //   }
  // }
  .el-select__tags {
    overflow-y: auto;
    overflow-x: auto;
  }
  .el-select__input{
    float:left;
  }
  .el-select__input:after{
    content:'.';
    clear:both;
    height:0;
    visibility:hidden;
    font-size:0;
    line-height:0;
  }
  .el-tag.error-tag {
    border: 1px solid @error-color-1;
    color: @error-color-1;
    background: rgba(231,51,113,.1);
    .el-tag__close {
      color: @error-color-1;
      &:hover {
        background-color: #ff4159;
        color: @fff;
      }
    }
    &:hover {
      border: 1px solid @error-color-1;
    }
  }
}

</style>
