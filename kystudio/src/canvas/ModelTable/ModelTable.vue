<template>
  <v-group ref="$modelTable" :config="groupStyle">
    <TableHeader
      :width="width"
      :table-alias="table.alias"
      :is-fact-table="table.type === 'FACT'"
    />
  </v-group>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'

import TableHeader from './TableHeader'

@Component({
  props: {
    id: {
      type: String,
      required: true
    },
    table: {
      type: Object,
      required: true
    },
    x: {
      type: Number,
      default: 0
    },
    y: {
      type: Number,
      default: 0
    },
    width: {
      type: Number,
      default: 0
    },
    draggable: {
      type: Boolean,
      default: false
    }
  },
  components: {
    TableHeader
  }
})
export default class ModelTable extends Vue {
  get groupStyle () {
    const { id, x, y, width, draggable } = this
    return { id, x, y, width, draggable }
  }
}
</script>
