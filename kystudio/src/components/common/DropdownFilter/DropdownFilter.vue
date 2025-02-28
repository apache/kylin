<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'

import locales from './locales'
import { getPickerOptions } from './handler'

@Component({
  props: {
    placement: {
      type: String,
      default: 'bottom-start'
    },
    width: {
      type: String,
      default: '200'
    },
    trigger: {
      type: String,
      default: 'hover'
    },
    type: {
      type: String,
      default: 'checkbox'
    },
    label: {
      type: String
    },
    value: {
      type: [String, Number, Array, Boolean, Date]
    },
    shortcuts: {
      type: Array,
      default: () => []
    },
    options: {
      type: Array,
      default: () => []
    },
    options2: {
      type: Array,
      default: () => []
    },
    hideArrow: {
      type: Boolean
    },
    isShowfooter: {
      type: Boolean,
      default: true
    },
    filterScrollMaxHeight: {
      type: String,
      default: '130'
    },
    isLoadingData: Boolean,
    isLoading: Boolean,
    loadingTips: String,
    isShowSearchInput: Boolean,
    filterPlaceholder: String,
    optionsTitle: String,
    totalSizeLabel: String,
    isShowDropDownImme: Boolean,
    isSelectAllOption: {
      type: Object,
      default: null
    }
  },
  locales
})
export default class DropdownFilter extends Vue {
  isShowDropDown = false
  startSec = 0
  endSec = 10
  searchValue = ''
  isAll = false
  timer = null

  get resetValue () {
    const { type } = this
    switch (type) {
      case 'checkbox': return []
      case 'inputNumber': return [null, null]
      default: return null
    }
  }

  get isPopoverType () {
    const { type } = this
    return ['checkbox', 'inputNumber'].includes(type)
  }

  get isDatePickerType () {
    const { type } = this
    return ['datetimerange'].includes(type)
  }

  get pickerOptions () {
    const { shortcuts } = this
    return {
      shortcuts: getPickerOptions(this)
        .filter(option => shortcuts.includes(option.type))
        .map(option => ({
          ...option,
          text: this.$t(option.type)
        }))
    }
  }

  handleInput (value) {
    if (value === null && this.isDatePickerType) {
      this.$emit('input', [])
    } else {
      this.$emit('input', value)
    }
  }

  handleChangeAll () {
    this.$emit('handleChangeAll')
  }
  saveLatencyRange () {
    const latencyFrom = this.startSec
    let latencyTo = null
    if (this.startSec > this.endSec) {
      latencyTo = this.endSec = this.startSec
    } else {
      latencyTo = this.endSec
    }
    this.handleInput([latencyFrom, latencyTo])
    this.handleToggleDropdown()
  }
  resetLatency () {
    this.startSec = 0
    this.endSec = 10
    this.handleClearValue()
    this.handleToggleDropdown()
  }

  handleClearValue () {
    this.$emit('input', this.resetValue)
  }

  handleSetDropdown (isShowDropDown) {
    this.isShowDropDown = isShowDropDown
    if (this.isDatePickerType) {
      !this.isShowDropDown && this.$refs.$datePicker.hidePicker()
    }
  }

  handleToggleDropdown () {
    this.handleSetDropdown(!this.isShowDropDown)
    if (this.isShowDropDown) {
      this.bindScrollEvent()
    }
  }

  removeScrollEvent () {
    const groupBlocks = this.$refs.$checkBoxGroup.querySelectorAll('.group-block .scroll-content')
    if (groupBlocks.length) {
      for (let group of groupBlocks) {
        group.removeEventListener('scroll', this.addScrollEvent, false)
      }
    }
  }
  bindScrollEvent () {
    const groupBlocks = this.$refs.$checkBoxGroup.querySelectorAll('.group-block .scroll-content')
    console.log(groupBlocks)
    if (groupBlocks.length) {
      for (let group of groupBlocks) {
        group.addEventListener('scroll', this.addScrollEvent, false)
      }
    }
  }

  handlerClickEvent () {
    this.$refs.$datePicker.$el.click()
  }

  addScrollEvent (e) {
    try {
      const scrollT = e.target.scrollTop
      if (scrollT > 0) {
        e.target.parentNode.className = 'group-block is-scrollable-top'
      } else {
        e.target.parentNode.className = 'group-block'
      }
      let scrollH = e.target.scrollHeight
      let clientH = e.target.clientHeight
      if (scrollT + clientH === scrollH) {
        this.$emit('filter-scroll-bottom')
      }
    } catch (e) {
      console.error(e)
    }
  }

  mounted () {
    this.isDatePickerType && this.$slots.default && this.$slots.default.length && this.$slots.default[0].elm.addEventListener('click', this.handlerClickEvent)
    if (this.isShowDropDownImme) {
      this.$nextTick(() => {
        this.handleSetDropdown(true)
      })
    }
  }

  beforeDestroy () {
    if (this.isDatePickerType) {
      this.$slots.default && this.$slots.default.length && this.$slots.default[0].elm.removeEventListener('click', this.handlerClickEvent)
    }
    if (this.isPopoverType) {
      this.removeScrollEvent()
    }
  }

  renderCheckboxGroup2 (h) {
    const { options2, optionsTitle2 } = this

    return (
      <div>
        {optionsTitle2 && <div class="group-title">{ optionsTitle2 }</div>}
        {options2.filter(o => {
          return !o.unavailable
        }).map(option => (
          <el-checkbox
            class="dropdown-filter-checkbox"
            key={option.value}
            label={option.value}>
            {option.renderLabel ? option.renderLabel(h, option) : option.label}
          </el-checkbox>
        ))}
        <div class="bottom-line"></div>
      </div>
    )
  }

  renderCheckboxGroup (h) {
    const { value, options, optionsTitle, options2, isSelectAllOption, isLoadingData, isLoading, loadingTips, filterScrollMaxHeight, totalSizeLabel, searchValue } = this
    return (
      <div class="filter-content" ref="$checkBoxGroup">
        {searchValue && (<div class="tatol-size">{ totalSizeLabel }</div>) }
        <el-checkbox-group value={value} onInput={this.handleInput}>
          {options2.length > 0 && this.renderCheckboxGroup2(h)}
          {optionsTitle && <div class="group-title">{ optionsTitle }</div>}
          {isSelectAllOption && (
            <div class="select-all-block">
              <el-checkbox
                class="select-all-checkbox"
                indeterminate={isSelectAllOption.indeterminate}
                onChange={this.handleChangeAll}
                key={isSelectAllOption.value}
                label={isSelectAllOption.value}>
                {isSelectAllOption.renderLabel ? isSelectAllOption.renderLabel(h, isSelectAllOption) : isSelectAllOption.label}
              </el-checkbox>
              <span class="select-num-tips">{isSelectAllOption.selectedSize}/{isSelectAllOption.totalSize}</span>
            </div>
          )}
          <div class="group-block">
            <div class="scroll-content" style={{'max-height': filterScrollMaxHeight + 'px'}}>
              {options.filter(o => {
                return !o.unavailable
              }).map(option => (
                <el-checkbox
                  class="dropdown-filter-checkbox"
                  key={option.value}
                  label={option.value}>
                  {option.renderLabel ? option.renderLabel(h, option) : option.label}
                </el-checkbox>
              ))}
              {isLoadingData && (
                <div class="loading-block">
                  {isLoading && <i class="el-ksd-n-icon-spinner-outlined"></i>}
                  {loadingTips && <span class="loading-tips">{ loadingTips }</span>}
                </div>
              )}
            </div>
          </div>
        </el-checkbox-group>
      </div>
    )
  }

  renderNoData (h) {
    const isShowImage = false
    return (
      <kylin-empty-data size="small" showImage={isShowImage} content={this.$t('kylinLang.common.noResults')}/>
    )
  }

  renderInputNumber (h) {
    const { value } = this
    if (value[0] && value[1]) {
      this.startSec = value[0]
      this.endSec = value[1]
    }
    return (
      <div class="latency-filter">
        <div class="latency-filter-pop">
          <el-input-number
            size="small"
            min={0}
            value={this.startSec}
            onInput={val1 => (this.startSec = val1)}></el-input-number>
          <span>&nbsp;S&nbsp;&nbsp;To</span>
          <el-input-number
            size="small"
            min={this.startSec}
            class="ksd-ml-10"
            value={this.endSec}
            onInput={val2 => (this.endSec = val2)}></el-input-number>
          <span>&nbsp;S</span>
        </div>
        <div class="latency-filter-footer">
          <el-button size="small" onClick={this.resetLatency}>{this.$t('kylinLang.query.clear')}</el-button>
          <el-button type="primary" onClick={this.saveLatencyRange} size="small">{this.$t('kylinLang.common.save')}</el-button>
        </div>
      </div>
    )
  }

  renderDatePicker (h) {
    const { value, pickerOptions } = this

    return (
      <div class="invisible-item" onClick={this.handleToggleDropdown}>
        <el-date-picker
          value={value}
          type="datetimerange"
          align="left"
          ref="$datePicker"
          start-placeholder={this.$t('startDatePlaceholder')}
          end-placeholder={this.$t('endDatePlaceholder')}
          onInput={this.handleInput}
          picker-options={pickerOptions}
          onBlur={() => this.handleSetDropdown(false)}
          onChange={() => this.handleSetDropdown(false)}>
        </el-date-picker>
      </div>
    )
  }

  renderFilterInput (h) {
    const { type, options, options2 } = this
    switch (type) {
      case 'checkbox': return [...options, ...options2].length ? this.renderCheckboxGroup(h) : this.renderNoData(h)
      case 'inputNumber': return this.renderInputNumber(h)
      default: return null
    }
  }

  filterFilters (v) {
    clearTimeout(this.timer)
    this.timer = setTimeout(() => {
      this.searchValue = v
      this.$emit('filterFilters', v)
      setTimeout(() => {
        this.bindScrollEvent()
      }, 500)
    }, 400)
  }

  renderPopover (h) {
    const { value, placement, width, trigger, isShowDropDown, hideArrow, isShowfooter, isShowSearchInput, filterPlaceholder, searchValue } = this

    return (
      <el-popover
        popper-class="dropdown-filter-popper"
        visible-arrow={!hideArrow}
        placement={placement}
        width={width}
        trigger={trigger}
        value={isShowDropDown}
        onInput={this.handleSetDropdown}>
        <div class="filter-value" slot="reference" onClick={this.handleToggleDropdown}>
          {this.$slots.default ? this.$slots.default : value}
          {!hideArrow && <i class={['el-icon-arrow-up', isShowDropDown && 'reverse']} />}
        </div>
        {isShowSearchInput && (
          <div class="search-input">
            <el-input
              placeholder={filterPlaceholder}
              onInput={v => this.filterFilters(v)}
              value={searchValue}>
              <i slot="prefix" class="el-input__icon el-icon-search"></i>
            </el-input>
          </div>
        )}
        <div class="body">
          {this.renderFilterInput(h)}
        </div>
        {isShowfooter && (
          <div class="footer">
            <el-button text type="primary" disabled={!value.length} onClick={this.handleClearValue}>
              {this.$t('clearSelectItems')}
            </el-button>
          </div>
        )}
      </el-popover>
    )
  }

  render (h) {
    const { label, value, isPopoverType, isShowDropDown, isDatePickerType, hideArrow } = this
    const labelProps = { domProps: { innerHTML: label } }

    return (
      <div class="dropdown-filter">
        <label class="filter-label">
          {label && <slot name="label" {...labelProps}></slot>}
        </label>
        {isPopoverType && (
          this.renderPopover(h)
        )}
        {isDatePickerType && (
          <div class="filter-value">
            {this.$slots.default ? this.$slots.default : value}
            {this.renderDatePicker(h)}
            {!hideArrow && <i class={['el-icon-arrow-up', isShowDropDown && 'reverse']} />}
          </div>
        )}
      </div>
    )
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';

.dropdown-filter {
  display: inline-block;
  font-size: 12px;
  .el-button--medium .button-text {
    overflow: hidden;
    text-overflow: ellipsis;
    max-width: 200px;
  }
  .filter-label {
    display: inline-block;
  }
  .filter-value {
    display: inline-block;
    position: relative;
    cursor: pointer;
    color: @color-text-primary;
  }
  .el-icon-arrow-up {
    transform: rotate(180deg);
  }
  .el-icon-arrow-up.reverse {
    transform: rotate(0);
  }
  .invisible-item {
    opacity: 0;
    position: absolute;
    top: 0;
    right: 0;
    bottom: 0;
    left: 0;
    cursor: pointer;
    overflow: hidden;
    z-index: -1;
    > * {
      position: absolute;
      bottom: 0;
      left: 0;
    }
    * {
      cursor: pointer !important;
    }
  }
}

.dropdown-filter-popper {
  padding: 0;
  width: unset !important;
  min-width: unset;
  .search-input {
    height: 34px;
    padding: 8px 0;
    border-bottom: 1px solid @ke-border-divider-color;
    .el-input__inner {
      border: none;
      &:active,
      &:focus {
        border: none !important;
        box-shadow: none !important;
      }
    }
  }
  .body {
    padding: 10px;
    position: relative;
    min-height: 22px;
    .tatol-size {
      font-size: 12px;
      line-height: 18px;
      text-align: center;
      margin-bottom: 8px;
      color: @text-disabled-color;
    }
    .bottom-line {
      border-top: 1px solid @ke-border-divider-color;
      margin: 0 -10px 12px;
    }
    .group-title {
      font-size: 12px;
      line-height: 18px;
      color: @text-disabled-color;
      margin-bottom: 8px;
    }
    .group-block {
      position: relative;
      .scroll-content {
        max-height: 180px;
        overflow: auto;
      }
      &.is-scrollable-top {
        &::before {
          content: none;
        }
        &::after {
          content: ' ';
          position: absolute;
          top: 0;
          left: -10px;
          right: -10px;
          height: 10px;
          background: linear-gradient(180deg, rgba(230, 235, 244, 0.8) 0%, rgba(230, 235, 244, 0) 100%);
        }
      }
    }
    .loading-block {
      height: 18px;
      margin-top: 8px;
      text-align: center;
      color: @text-placeholder-color;
      .el-ksd-n-icon-spinner-outlined {
        font-size: 14px;
      }
      .loading-tips {
        font-size: 12px;
        line-height: 18px;
        position: relative;
        &::after,
        &::before {
          content: "";
          position: absolute;
          top: 50%;
          background: @text-placeholder-color;
          height: 1px;
          width: 28px;
        }
        &::after {
          right: -36px;
        }
        &::before {
          left: -36px;
        }
      }
    }
  }
  .footer {
    padding: 12px 10px;
    border-top: 1px solid @ke-border-divider-color;
  }
  .el-checkbox {
    display: flex;
    &:not(:last-child) {
      margin-bottom: 8px;
    }
    .el-checkbox__label {
      font-size: 12px;
    }
    .select-all-checkbox {
      margin-bottom: 4px;
    }
  }
  .select-all-block {
    display: flex;
    .select-num-tips {
      height: 22px;
      width: 100%;
      display: inline-block;
      font-size: 12px;
      line-height: 18px;
      text-align: right;
      color: @text-placeholder-color;
    }
  }
  .el-checkbox + .el-checkbox {
    margin-left: 0;
  }
  .el-button.is-text {
    padding: 0;
    font-size: 12px;
  }
}
</style>
