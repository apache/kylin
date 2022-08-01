<template>
  <table class="ksd-table table-ext-info">
    <tr v-if="table.name">
      <th>TABLE NAME</th>
      <td>{{table.name}}</td>
    </tr>
    <tr v-if="table.database">
      <th>HIVE DATABASE</th>
      <td>{{table.database}}</td>
    </tr>
    <tr v-if="table.table_type">
      <th>TABLE TYPE</th>
      <td>{{table.table_type}}</td>
    </tr>
    <tr v-if="table.database">
      <th>SNAPSHOT TIME</th>
      <td>{{table.database}}</td>
    </tr>
    <tr v-if="table.data_source_properties">
      <th>LOCATION</th>
      <td>{{table.data_source_properties.location}}</td>
    </tr>
    <tr v-if="table.data_source_properties">
      <th>INPUT FORMAT</th>
      <td>{{table.data_source_properties.hive_inputFormat}}</td>
    </tr>
    <tr v-if="table.data_source_properties">
      <th>OUT FORMAT</th>
      <td>{{table.data_source_properties.hive_outputFormat}}</td>
    </tr>
    <tr v-if="table.data_source_properties">
      <th>OWNER</th>
      <td>{{table.data_source_properties.owner}}</td>
    </tr>
    <tr v-if="table.data_source_properties">
      <th>TOTAL FILE NUMBER</th>
      <td>{{table.data_source_properties.total_file_number}}</td>
    </tr>
    <tr v-if="table.data_source_properties">
      <th>TOTAL FILE SIZE</th>
      <td>{{table.data_source_properties.total_file_size}}</td>
    </tr>
    <tr v-if="table.data_source_properties">
      <th>PARTITIONED</th>
      <td>{{table.data_source_properties.partition_column!==''}}</td>
    </tr>
    <tr v-if="table.data_source_properties">
      <th>PARTITION COLUMNS</th>
      <td>{{table.data_source_properties.partition_column}}</td>
    </tr>
  </table>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'

import locales from './locales'

@Component({
  props: {
    table: {
      type: Object
    }
  },
  locales
})
export default class TableExtInfo extends Vue {
}
</script>

<style lang="less">
@import '../../../../assets/styles/variables.less';
</style>
