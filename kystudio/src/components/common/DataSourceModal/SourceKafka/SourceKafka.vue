<template>
  <div class="create-kafka">
    <el-alert
      :title="$t('errorCodeTips')"
      :description="errorCode"
      type="error"
      :closable="false"
      class="ksd-mb-8"
      v-if="isShowErrorBrokers"
      show-icon>
    </el-alert>
    <div class="ksd-title-label-small">{{$t('inputClusterInfo')}}</div>
    <el-row v-for="(c, index) in kafkaMeta.clusters" :key="index" class="ksd-mt-10">
      <!-- <el-col :span="2">
        <div class="cluster-id">{{$t('cluster')}}{{index+1}}</div>
      </el-col> -->
      <el-col :span="24">
        <div>
          <arealabel
            :duplicateremove="false"
            :validateRegex="validateRegex"
            :errorValues="failed_servers"
            :validateFailedMove="false"
            :isNeedNotUpperCase="true"
            :isSignSameValue="true"
            :selectedlabels="brokers"
            @duplicateTags="checkDuplicateValue"
            @validateFailedTags="checkValidateValue"
            @refreshData="refreshData"
            @change="getLastFiveBrokers"
            :disabled="loading"
            splitChar=","
            :allowcreate="true"
            :placeholder="$t('brockerTips')"
            @removeTag="removeSelected"
            :isCache="true"
            :labels="lastFiveBrokers"
            :datamap="{label: 'label', value: 'value'}">
          </arealabel>
        </div>
        <div class="error-msg" v-if="isHaveDupBlokers">{{$t('dupBlokersTips')}}</div>
        <div class="error-msg" v-if="isHaveErrorBlokers">{{$t('errorBlokersTips')}}</div>
      </el-col>
      <!-- <el-col :span="2">
        <div class="cluster-action ky-no-br-space">
          <el-button type="primary" plain circle icon="el-icon-ksd-add_2" class="ksd-ml-10" size="mini"  @click="addNewCluster"></el-button>
          <el-button plain icon="el-icon-minus" size="mini" circle :disabled="kafkaMeta.clusters.length==1" @click="deleteCluster(index)" class="del-pro ksd-ml-5"></el-button>
        </div>
      </el-col> -->
    </el-row>
    <el-button type="primary" size="small" class="ksd-mt-20" :disabled="!brokers.length || isHaveDupBlokers || isHaveErrorBlokers" @click="getClusterInfo" :loading="loading">{{$t('getClusterInfo')}}</el-button>
    <div v-loading="topicDetailLoading">
      <el-row :gutter="10" class='json-box ksd-mtb-20'>
        <el-col :span='9' >
          <div class="topic-box">
            <div class="ksd-title-label-small ksd-mb-10">{{$t('topicTitle')}}</div>
            <el-input prefix-icon="el-icon-search" v-model="searchStr" :placeholder="$t('searchTopicPlaceholder')" size="small" style="width:100%" class="ksd-mb-10"></el-input>
            <div v-scroll class="topic-list" >
              <el-tree :data="pagerTreeData" default-expand-all show-overflow-tooltip :empty-text="$t('noTopic')" :props="treeProps" @node-click="getTopicInfo">
                <div class="custom-tree-node" slot-scope="{ node, data }">
                  <div :class="{'load-more': data.loadMore, 'is-selected': data.isSelected}">{{ node.label }}</div>
                </div>
              </el-tree>
            </div>
          </div>
          <div class="selected-topics ksd-mt-4" v-if="kafkaMeta.subscribe">{{$t('selected')}}{{kafkaMeta.subscribe}}</div>
        </el-col>
        <el-col :span='15' style="position:relative;">
          <div class="ksd-mb-10 clearfix">
            <span class="ksd-fleft ksd-title-label-small">{{$t('sampleData')}}</span>
            <span class="ksd-fright refresh-btn" :class="{'disabled': !sourceSchema}" @click='nextMessage'>
              <i class="el-icon-ksd-table_refresh"></i>
              <span>{{$t('next')}}</span>
            </span>
          </div>
          <kylin-editor v-model="sourceSchema" @input="handleParseKafkaData" ref="jsonDataBox" :dragable="false" lang="json" theme="chrome" width="460px" height="300"></kylin-editor>
          <div class="json-empty-tips" v-if="!sourceSchema&&!isEmptyTopic&&!isShowError">
            {{$t('emptyDataTips')}}
          </div>
          <div class="json-error-tips2 ksd-mt-4" v-if="isShowError">
            <i class="el-icon-ksd-error_01"></i>
            {{$t('jsonDataErrorTips')}}
          </div>
          <div class="json-error-tips" v-if="isEmptyTopic">
            {{$t('emptyTopicTips')}}
          </div>
        </el-col>
      </el-row>
      <div class="ksd-title-label-small ksd-mb-4">{{$t('parserName')}}</div>
      <el-input v-model="kafkaMeta.parser_name" disabled size="medium" @input="resetParser"></el-input>
      <!-- <el-button :disabled="!sourceSchema || !kafkaMeta.parserName" :loading="convertLoading" @click='streamingOnChange()' size="small" type="primary" class="convert-btn ksd-mt-10">{{$t('parse')}}</el-button> -->
      <div v-if="isParserErrorShow" class="parser-msg ksd-mt-10">
        <i class="el-icon-ksd-error_01"></i>
        <span>{{parserErrorMsg}}</span>
      </div>
    </div>
  </div>
</template>

<script>
import Vue from 'vue'
import { mapGetters, mapActions } from 'vuex'
import { Component } from 'vue-property-decorator'
import locales from './locales'
import TreeList from '../../TreeList'
import arealabel from '../../area_label.vue'
import { handleSuccess, handleError, objectClone, ArrayFlat } from 'util'
@Component({
  props: {
    convertDataStore: {
      default: () => null
    },
    sampleDataStore: {
      default: () => null
    },
    treeDataStore: {
      default: () => []
    }
  },
  components: {
    TreeList,
    arealabel
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject',
      'selectedProjectDatasource'
    ])
  },
  methods: {
    ...mapActions({
      clusterInfo: 'GET_CLUSTER_INFO',
      topicInfo: 'GET_TOPIC_INFO',
      loadDatabase: 'LOAD_KAFKA_DATABASE',
      loadClusters: 'GET_CLUSTERS',
      convertTopicJson: 'CONVERT_TOPIC_JSON',
      validateTableSchema: 'VALIDATE_TABLE_SCHEMA'
    })
  },
  locales
})
export default class SourceKafka extends Vue {
  kafkaMeta = {
    database: '',
    name: '',
    subscribe: '',
    parser_name: 'org.apache.kylin.parser.TimedJsonStreamParser',
    clusters: [{
      brokers: []
    }],
    has_shadow_table: false,
    batch_table_identity: '',
    starting_offsets: 'latest'
  }
  brokers = []
  validateRegex = /^(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1,2}|1\d\d|2[0-4]\d|25[0-5]):([0-9]|[1-9]\d{1,3}|[1-5]\d{4}|6[0-5]{2}[0-3][0-5])$/
  showTopicBox = false
  topicDetailLoading = false
  treeProps = {
    children: 'children',
    label: 'label'
  }
  treeData = []
  loading = false
  sampleData = null
  topicOptions = null
  streamingMeta = {name: '', type: 'kafka'}
  topicCount = 0
  searchStr = ''
  sourceSchema = ''
  isShowError = false
  isEmptyTopic = false
  convertLoading = false
  isParserErrorShow = false
  parserErrorMsg = ''
  isHaveDupBlokers = false
  isHaveErrorBlokers = false
  failed_servers = []
  isShowErrorBrokers = false
  lastFiveBrokers = []
  checkDuplicateValue (val) {
    this.isHaveDupBlokers = val
  }
  checkValidateValue (val) {
    this.isHaveErrorBlokers = val
  }
  refreshData (brokers) {
    this.kafkaMeta.clusters[0].brokers = brokers
    this.brokers = brokers
    // brokers.forEach((b, index) => {
    //   this.kafkaMeta.clusters[0].brokers.push({ id: index, host: b.trim().split(':')[0], port: b.trim().split(':')[1] })
    // })
    this.showTopicBox = false
    this.resetForm()
    this.handleParseKafkaData()
  }
  removeSelected () {}
  addNewCluster () {
    this.kafkaMeta.clusters.push({brokers: []})
  }
  deleteCluster (index) {
    this.kafkaMeta.clusters.splice(index, 1)
  }
  nextMessage () {
    this.isShowError = false
    this.resetParser()
    const message = this.sampleData.message
    const msgLength = this.sampleData.message.length
    this.currentMessageIndex++
    if (this.currentMessageIndex > msgLength - 1) {
      this.currentMessageIndex = 0
    }
    if (this.sampleData.message_type === 'binary') {
      this.sourceSchema = message[this.currentMessageIndex]
      this.isShowError = true
    } else if (this.sampleData.message_type === 'json') {
      this.sourceSchema = JSON.stringify(JSON.parse(message[this.currentMessageIndex]), null, '\t')
      this.isShowError = false
    }
    this.handleParseKafkaData()
  }
  handleParseKafkaData () {
    const convertData = objectClone(this.topicOptions)
    if (convertData) {
      convertData.kafka.kafka_config = this.kafkaData(this.kafkaMeta)
      convertData.kafka.message = this.sourceSchema
      convertData.kafka.message_type = this.sampleData && this.sampleData.message_type
    }
    this.$emit('input', { convertData, sampleData: this.sampleData, treeData: this.treeData })
  }
  resetParser () {
    this.isParserErrorShow = false
    this.isEmptyColName = false
  }
  getLastFiveBrokers () {
    const cacheBrokers = JSON.parse(localStorage.getItem('cacheTags'))
    this.lastFiveBrokers = cacheBrokers.map(b => {
      return {label: b, value: b}
    })
  }
  get searchTreeData () {
    let filterResult = objectClone(this.treeData) || []
    if (this.searchStr) {
      filterResult.forEach((d) => {
        d.children = d.children.filter((x) => {
          return x.label.indexOf(this.searchStr) >= 0
        })
      })
    }
    return filterResult
  }
  get pagerTreeData () {
    const pagerData = objectClone(this.searchTreeData)
    pagerData.forEach((d, index) => {
      d.children = d.children.slice(0, (d.currentPage + 1) * 50)
      if (d.children.length < this.searchTreeData[index].children.length) {
        d.children.push({
          label: this.$t('loadMore'),
          loadMore: true,
          index: index,
          currentPage: d.currentPage
        })
      }
    })
    return pagerData
  }
  resetForm () {
    var editor = this.$refs.jsonDataBox && this.$refs.jsonDataBox.editor
    editor && editor.setValue(' ')
    this.sourceSchema = ''
    this.treeData = []
    this.topicDetailLoading = false
    this.loading = false
    this.columnList = []
    this.convertLoading = false
    this.isShowError = false
    this.isEmptyTopic = false
    this.kafkaMeta.subscribe = ''
    this.isShowErrorBrokers = false
    this.errorCode = ''
    this.failed_servers = []
  }
  kafkaData (kafkaMeta) {
    const kafkaMetaObj = objectClone(kafkaMeta)
    const broders = ArrayFlat(kafkaMetaObj.clusters.map(it => it.brokers))
    kafkaMetaObj.kafka_bootstrap_servers = broders.join(',')
    delete kafkaMetaObj.clusters
    return kafkaMetaObj
  }
  getClusterInfo () {
    this.resetForm()
    this.loading = true
    const kafkaMetaObj = this.kafkaData(this.kafkaMeta)
    this.clusterInfo({
      project: this.currentSelectedProject,
      kafka_config: kafkaMetaObj
    }).then((res) => {
      handleSuccess(res, (result) => {
        this.showTopicBox = true
        var data = result || []
        let treeNode = []
        this.topicCount = 0
        let clusterIndex = -1
        Object.keys(data).forEach((cluster) => {
          clusterIndex++
          this.topicCount = this.topicCount + data[cluster].length
          treeNode.push({
            label: cluster,
            currentPage: 0,
            children: data[cluster].map((topic) => {
              return {label: topic, clusterIndex: clusterIndex}
            })
          })
        })
        this.treeData = treeNode
      })
      this.loading = false
    }, (res) => {
      // handleError(res)
      if (!res.data.data || (res.data.data.failed_servers && !res.data.data.failed_servers.length)) {
        handleError(res)
      } else {
        this.isShowErrorBrokers = true
        this.$nextTick(() => {
          this.errorCode = `Error Code - ${res.data.msg.split('(')[0]}`
          this.failed_servers = res.data.data.failed_servers
        })
      }
      this.loading = false
    })
  }
  getTopicInfo (topic) {
    if (topic.loadMore) {
      this.treeData[topic.index].currentPage++
    } else {
      this.resetParser()
      let selectedTreeIndex
      let selectedTopicIndex
      this.treeData.forEach((t, index) => {
        this.treeData[index].children.forEach((d, indexC) => {
          if (d.label === topic.label) {
            selectedTreeIndex = index
            selectedTopicIndex = indexC
          }
          this.$set(this.treeData[index].children[indexC], 'isSelected', false)
        })
      })
      this.$set(this.treeData[selectedTreeIndex].children[selectedTopicIndex], 'isSelected', true)
      this.kafkaMeta.subscribe = topic.label
      const kafkaMetaObj = this.kafkaData(this.kafkaMeta)
      this.clusterIndex = topic.clusterIndex
      this.topicOptions = {
        name: topic.label,
        kafka: {
          project: this.currentSelectedProject,
          kafka_config: kafkaMetaObj,
          cluster_index: this.clusterIndex
        }
      }
      this.topicDetailLoading = true
      this.sampleData = null
      this.sourceSchema = ''
      this.isShowError = false
      this.isEmptyTopic = false
      this.currentMessageIndex = -1
      this.topicInfo(this.topicOptions).then((res) => {
        handleSuccess(res, (data) => {
          this.topicDetailLoading = false
          if (data) {
            this.currentMessageIndex++
            this.sampleData = data
            if (data.message_type === 'json' && data.message && data.message[this.currentMessageIndex]) {
              this.sourceSchema = JSON.stringify(JSON.parse(data.message[this.currentMessageIndex]), null, '\t')
              this.isShowError = false
            } else if (data.message_type === 'binary' && data.message && data.message[this.currentMessageIndex]) {
              this.sourceSchema = data.message[this.currentMessageIndex]
              this.isShowError = true
            }
          } else {
            this.isEmptyTopic = true
            this.sampleData = null
            this.sourceSchema = ' '
            this.$nextTick(() => { // 清空编辑器黑科技
              this.sourceSchema = ''
            })
          }
          this.handleParseKafkaData()
        })
      }, (res) => {
        this.topicDetailLoading = false
        handleError(res)
      }).catch((res) => {
        this.topicDetailLoading = false
        this.isShowError = true
      })
    }
  }
  mounted () {
    this.brokers = []
    this.getLastFiveBrokers()
    if (this.convertDataStore && this.sampleDataStore && this.treeDataStore.length) {
      this.topicOptions = objectClone(this.convertDataStore)
      this.kafkaMeta = this.convertDataStore.kafka.kafka_config
      this.brokers = [this.kafkaMeta.kafka_bootstrap_servers]
      this.kafkaMeta.clusters = [{brokers: [this.kafkaMeta.kafka_bootstrap_servers]}]
      this.showTopicBox = true
      this.treeData = this.treeDataStore
      this.sourceSchema = JSON.stringify(JSON.parse(this.convertDataStore.kafka.message), null, '\t')
      this.sampleData = this.sampleDataStore
      this.currentMessageIndex = this.sampleData.message.indexOf(this.convertDataStore.kafka.message)
    }
  }
}
</script>

<style lang="less">
@import '../../../../assets/styles/variables.less';
.create-kafka {
  .cluster-id,
  .cluster-action {
    height: 30px;
    line-height: 30px;
  }
  .cluster-action {
    position: relative;
    top: 3px;
  }
  .error-msg {
    color: @color-danger;
    font-size: 12px;
  }
  .json-box {
    .selected-topics {
      color: @ke-color-primary;
      margin-left: 5px;
    }
    .topic-list {
      border: 1px solid @ke-border-secondary;
      border-radius: 6px;
      height: 260px;
      .el-tree__empty-block {
        min-height: 260px;
        .el-tree__empty-text {
          width: 80%;
        }
      }
      .el-tree-node__content {
        .custom-tree-node {
          width: 100%;
          .is-selected {
            width: 100%;
            background-color: @ke-color-info-bg;
            height: 34px;
            line-height: 34px;
            margin-left: -42px;
            padding-left: 42px;
          }
        }
      }
    }
    .json-error-tips,
    .json-empty-tips {
      position: absolute;
      position: absolute;
      top: 81px;
      left: 6px;
      z-index: 9;
      height: 108px;
      width: 460px;
      text-align: center;
      font-size: 14px;
      color: @text-title-color;
      padding-top: 80px;
      i {
        color: @error-color-1;
        font-size: 16px;
      }
    }
    .json-error-tips2 {
      font-size: 12px;
      i {
        color: @error-color-1;
        font-size: 14px;
      }
    }
    .json-empty-tips {
      color: @text-disabled-color;
    }
  }
  .refresh-btn {
    color: @base-color;
    cursor: pointer;
    &.disabled {
      color: @text-disabled-color;
      cursor: not-allowed;
    }
  }
}
</style>
