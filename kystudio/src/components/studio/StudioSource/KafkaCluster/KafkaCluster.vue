<template>
  <div>
    <el-form  label-position='right'   label-width='120px' ref='kafkaForm'>
        <!-- <div class="ksd-title-label-small ksd-mb-10">{{$t('clusterInfo')}}</div> -->
        <!-- <el-table class='table_margin' border
          :data="brokers"
          style='width: 100%'>
          <el-table-column
            show-overflow-tooltip
            label="ID"
            type="index"
            align="center"
            width='80'>
          </el-table-column>
          <el-table-column
            show-overflow-tooltip
            :label="$t('clusterInfo')">
            <template slot-scope='scope'>
              <div class="tags">
                <el-tag size="small">{{scope.row.blocker}}</el-tag>
              </div>
            </template>
          </el-table-column>
        </el-table> -->
        <table class="ksd-table ksd-mb-20">
          <tr>
            <th>{{$t('kafkaBroker')}}</th>
            <td><el-tag size="small">{{streamingData.kafka_bootstrap_servers}}</el-tag></td>
          </tr>
          <tr>
            <th>{{$t('topic')}}</th>
            <td>{{streamingData.subscribe}}</td>
          </tr>
          <tr>
            <th>{{$t('parserName')}}</th>
            <td>org.apache.kylin.parser.TimedJsonStreamParser</td>
            <!-- <td>{{streamingData.parserName}}</td> -->
          </tr>
          <!-- <tr>
            <th>{{$t('timestampField')}}</th>
            <td>{{streamingData.timestampField}}</td>
          </tr>
          <tr>
            <th>{{$t('parserProperties')}}</th>
            <td>{{streamingData.parserProperties}}</td>
          </tr> -->
          <tr>
            <th>{{$t('hiveTable')}}</th>
            <td>{{streamingData.batch_table_identity}}</td>
          </tr>
        </table>
    </el-form>
    </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import locales from './locales'
@Component({
  props: {
    streamingData: {
      type: Object
    }
  },
  locales
})
export default class KafkaCluster extends Vue {
}
</script>
