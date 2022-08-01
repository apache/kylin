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
    hideArrow: {
      type: Boolean
    }
  },
  locales
})
export default class DropdownFilter extends Vue {
  isShowDropDown = false

  get resetValue () {
    const { type } = this
    switch (type) {
      case 'checkbox': return []
      default: return null
    }
  }

  get isPopoverType () {
    const { type } = this
    return ['checkbox'].includes(type)
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
  }

  handlerClickEvent () {
    this.$refs.$datePicker.$el.click()
  }

  mounted () {
    this.isDatePickerType && this.$slots.default && this.$slots.default.length && this.$slots.default[0].elm.addEventListener('click', this.handlerClickEvent)
  }

  beforeDestroy () {
    if (this.isDatePickerType) {
      this.$slots.default && this.$slots.default.length && this.$slots.default[0].elm.removeEventListener('click', this.handlerClickEvent)
    }
  }

  renderCheckboxGroup (h) {
    const { value, options } = this

    return (
      <el-checkbox-group value={value} onInput={this.handleInput}>
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
      </el-checkbox-group>
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
          start-placeholder="开始日期"
          end-placeholder="结束日期"
          onInput={this.handleInput}
          picker-options={pickerOptions}
          onBlur={() => this.handleSetDropdown(false)}
          onChange={() => this.handleSetDropdown(false)}>
        </el-date-picker>
      </div>
    )
  }

  renderFilterInput (h) {
    const { type } = this
    switch (type) {
      case 'checkbox': return this.renderCheckboxGroup(h)
      default: return null
    }
  }

  renderPopover (h) {
    const { value, placement, width, trigger, isShowDropDown, hideArrow } = this

    return (
      <el-popover
        popper-class="dropdown-filter-popper"
        placement={placement}
        width={width}
        trigger={trigger}
        value={isShowDropDown}
        onInput={this.handleSetDropdown}>
        <div class="filter-value" slot="reference" onClick={this.handleToggleDropdown}>
          {this.$slots.default ? this.$slots.default : value}
          {!hideArrow && <i class={['el-icon-arrow-up', isShowDropDown && 'reverse']} />}
        </div>
        <div class="body">
          {this.renderFilterInput(h)}
        </div>
        <div class="footer">
          <el-button text type="primary" disabled={!value.length} onClick={this.handleClearValue}>
            {this.$t('clearSelectItems')}
          </el-button>
        </div>
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
  .filter-label {
    display: inline-block;
  }
  .filter-value {
    display: inline-block;
    position: relative;
    cursor: pointer;
    color: @color-text-primary;
    // &:hover,
    // &:hover i {
    //   color: @color-primary;
    // }
  }
  // .filter-value i {
  //   margin-left: 5px;
  //   color: #989898;
  // }
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
  .body {
    padding: 10px;
  }
  .footer {
    padding: 0 10px 10px 10px;
  }
  .el-checkbox {
    display: flex;
    &:not(:last-child) {
      margin-bottom: 10px;
    }
    .el-checkbox__label {
      font-size: 12px;
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
