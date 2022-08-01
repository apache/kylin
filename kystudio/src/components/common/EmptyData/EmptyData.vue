<template>
  <div class="empty-data" :class="emptyClass">
    <div class="center">
      <img :src="emptyImageUrl" :class="{'large-icon': size === 'large'}" v-if="showImage"/>
    </div>
    <div class="center">
      <div v-html="emptyContent"></div>
    </div>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'

import locales from './locales'
import defaultImage from '../../../assets/img/empty/empty_state_empty.svg'

@Component({
  props: {
    image: {
      type: String
    },
    content: {
      type: String
    },
    size: {
      type: String
    },
    showImage: {
      type: Boolean,
      default: true
    }
  },
  locales
})
export default class EmptyData extends Vue {
  get emptyImageUrl () {
    return this.image || defaultImage
  }
  get emptyContent () {
    return this.content || this.$t('content')
  }
  get emptyClass () {
    return this.size ? 'empty-data-' + this.size : 'empty-data-normal'
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';
.empty-data {
  pointer-events:none;
  position: absolute;
  top: 30%;
  left: 50%;
  transform: translate(-50%, -30%);
  color: @text-placeholder-color;
  .center {
    text-align: center;
    max-width: 392px;
  }
  .center:first-child {
    margin-bottom: 16px;
  }
  &.empty-data-normal {
    img {
      height: 120px;
    }
    font-size: 14px;
  }
  &.empty-data-small {
    img {
      height: 60px;
    }
    font-size: 14px;
    .center:first-child {
      margin-bottom: 8px;
    }
  }
}
</style>
