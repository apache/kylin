<template>
  <div class="dashboard" v-loading="isLoading">
    <header class="dashboard-header">
      <h1 class="ksd-title-label" v-if="projectSettings">{{projectSettings.project || projectSettings.alias}}</h1>
      <div id="datepicker">
        <date-range-picker v-model="pickerDates" :locale-data="{format: 'yyyy-mm-dd'}" @update="changeDateRange">
          <template v-slot:input="picker" style="min-width: 350px;" >
            {{ pickerDates.startDate }} - {{ pickerDates.endDate }}
          </template>
        </date-range-picker>
      </div>
    </header>

    <section class="dashboard-body" v-if="projectSettings">
      <!-- Model Metrics  -->
      <div class = "row">
        <div class="el-col-sm-1">
          <el-tooltip placement="bottom" :content="'As of ' + currentTime">
          <div class="square-big" tool-palcement="bottom">
            <div class = "title">
              TOTAL MODEL COUNT
            </div>
            <div class="metric" v-if="totalModel || totalModel === 0">
              {{totalModel}}
            </div>
            <div class="metric" v-if="!totalModel && (totalModel !== 0)">
              --
            </div>
            <a class="description" @click="toModel()">
              More Details
            </a>
          </div>
          </el-tooltip>
          <el-tooltip placement="bottom">
            <div slot="content">{{"Max:" + maxExpansionRate + " | Min:" + minExpansionRate}}<br/>{{"As of " + currentTime}}</div>
            <div class="square-big" tool-palcement="bottom">
              <div class = "title">
                AVG MODEL EXPANSION
              </div>
              <div class="metric" v-if="avgExpansionRate || avgExpansionRate === 0">
                {{avgExpansionRate}}
              </div>
              <div class="metric" v-if="!avgExpansionRate && (avgExpansionRate !== 0)">
                --
              </div>
              <a class="description" @click="toModel()">
                More Details
              </a>
            </div>
          </el-tooltip>
        </div>
      </div>
      <!-- Query Metrics  -->
      <div class = "col-sm-10">
        <div class = "row">
          <div class = "col-sm-2" style="width: 20%">
            <div class = "square" :class="{'square-active': currentSquare === 'queryCount'}" @click="queryCountChart()">
              <div class = "title">
                QUERY<br/>COUNT
              </div>
              <div class = "metric" v-if="queryCount || queryCount === 0">
                {{queryCount}}
              </div>
              <div class="metric" v-if="!queryCount && (queryCount !== 0)">
                --
              </div>
              <a class="description" @click="toQuery()">
                More Details
              </a>
            </div>
          </div>
          <div class="col-sm-2" style="width: 20%">
            <div class="square1" :class="{'square-active': currentSquare === 'queryAvg'}" @click="queryAvgChart()" >
              <div class="title">
                AVG QUERY LATENCY
              </div>
              <div class="metric" v-if="avgQueryLatency || avgQueryLatency === 0">
                {{numFilter(avgQueryLatency / 1000)}}<span class="unit"> sec</span>
              </div>
              <div class="metric" v-if="!avgQueryLatency && (avgQueryLatency !== 0)">
                --
              </div>
              <a class="description" @click="toQuery()">
                More Details
              </a>
            </div>
          </div>
          <!--  Job Metrics  -->
          <div class="col-sm-2" style="width: 20%">
            <div class="square2" :class="{'square-active': currentSquare === 'jobCount'}" @click="jobCountChart()" >
              <div class="title">
                JOB<br/>COUNT
              </div>
              <div class="metric" v-if="jobCount || jobCount === 0">
                {{jobCount}}
              </div>
              <div class="metric" v-if="!avgQueryLatency && (avgQueryLatency !== 0)">
                --
              </div>
              <a class="description" @click="toJob()">
                More Details
              </a>
            </div>
          </div>
          <div class="col-sm-2" style="width: 20%">
            <div class="square3" :class="{'square-active': currentSquare === 'jobAvgBuild'}" @click="jobAvgBuildChart()" >
              <div class="title">
                AVG BUILD TIME PER {{jobUnit}}
              </div>
              <div class="metric" v-if="jobAvgBuildTime || jobAvgBuildTime === 0">
                {{jobAvgBuildTime}}<span class="unit"> {{jobTimeUnit}}</span>
              </div>
              <div class="metric" v-if="!avgQueryLatency && (avgQueryLatency !== 0)">
                --
              </div>
              <a class="description" @click="toJob()">
                More Details
              </a>
            </div>
          </div>
        </div>
      </div>
      <!--  charts  -->
      <div class="row">
        <div class="col-sm-2" v-if="barChartOptions">
          <div class="barChartSquare">
            <div class="form-control">
              Show Value: <input type="checkbox" v-model="barChartOptions.series[0].label.show" @click="showBarChartValue()">
            </div>
            <BarEcharts :options="barChartOptions"/>
          </div>
        </div>
        <div class="col-sm-2" v-if="lineChartOptions">
          <div class="lineChartSquare">
              <select class="select-control"
                      v-model="currentSelectFilter"
                      @change="getCurrentSelectedLineChart">
                <option v-for="filter in chartFilter" :value="filter.name">
                  {{filter.name}}
                </option>
              </select>
            <LineEcharts :options="lineChartOptions"/>
          </div>
        </div>
      </div>
    </section>
    <kylin-empty-data v-else>
    </kylin-empty-data>
  </div>
</template>

<script>
import Vue from 'vue'
import {mapActions, mapGetters} from 'vuex'
import { Component } from 'vue-property-decorator'
import {handleError, handleSuccessAsync} from '../../util/index'
import DateRangePicker from 'vue2-daterange-picker'
import 'vue2-daterange-picker/dist/vue2-daterange-picker.css'
import moment from 'moment'
import baseOptions from './chartOption'
import BarEcharts from './BarEcharts'
import LineEcharts from './LineEcharts'
Vue.component('date-range-picker', DateRangePicker)

@Component({
  methods: {
    ...mapActions({
      getModelStatistics: 'GET_MODEL_STATISTICS',
      getQueryStatistics: 'GET_QUERY_STATISTICS',
      fetchProjectSettings: 'FETCH_PROJECT_SETTINGS',
      getChartData: 'GET_CHART_DATA',
      getJobStatistics: 'GET_JOB_STATISTICS'
    })
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject',
      'currentProjectData'
    ])
  },
  components: {DateRangePicker, BarEcharts, LineEcharts},
  watch: {
    pickerDates: {
      handler (newValue, oldValue) {
      },
      deep: true
    },
    barChartOptions: {
      handler (newValue, oldValue) {
      },
      deep: true
    },
    lineChartOptions: {
      handler (newValue, oldValue) {
      },
      deep: true
    },
    currentSelectFilter: {
      handler (newValue, oldValue) {
      },
      deep: true
    }
  }
})

export default class Dashboard extends Vue {
  // name = 'Dashboard'
  isLoading = false
  projectSettings = null;
  totalModel = 0;
  avgExpansionRate = 0;
  minExpansionRate = 0;
  maxExpansionRate = 0;
  currentTime = moment().format('YYYY-MM-DD');
  queryCount = 0;
  avgQueryLatency = 0;
  currentSquare = '';
  pickerDates = {
    startDate: '',
    endDate: ''
  };
  options = null
  // category: JOB, QUERY
  // metric: QUERY:{count, avg_query_latency}
  // dimension : filter by model or date(day, week)
  barChart = null;
  lineChart = null;
  barChartOptions = null;
  lineChartOptions = null;
  category = ['QUERY', 'JOB'];
  metric = [
    {name: 'query count', value: 'QUERY_COUNT'},
    {name: 'avg query latency', value: 'AVG_QUERY_LATENCY'},
    {name: 'job count', value: 'JOB_COUNT'},
    {name: 'avg build time', value: 'AVG_JOB_BUILD_TIME'}
  ];
  dimension = [
    {name: 'project', value: 'PROJECT'},
    {name: 'model', value: 'MODEL'},
    {name: 'day', value: 'DAY'},
    {name: 'week', value: 'WEEK'},
    {name: 'month', value: 'MONTH'}
  ];
  chartFilter = [
    {name: 'Daily', value: 'day'},
    {name: 'Weekly', value: 'week'},
    {name: 'Monthly', value: 'month'}
  ];
  currentSelectFilter = this.chartFilter[1].name
  barChartCategory = null;
  barChartMetric = null;
  lineChartCategory = null;
  lineChartMetric = null;
  chartType = '';
  barChartDimension = this.dimension[1].value;
  lineChartDimension = this.dimension[3].value;
  jobUnit = 'MB';
  jobTimeUnit = 'sec';
  jobCount = 0;
  jobTotalByteSize = 0;
  jobTotalLatency = 0;
  jobAvgBuildTime = 0;
  toModel () {
    this.$router.push('/studio/model')
  }
  toQuery () {
    this.$router.push('/query/queryhistory')
  }
  toJob () {
    this.$router.push('/monitor/job')
  }
  showLoading () {
    this.isLoading = true
  }
  changeDateRange (val) {
    this.pickerDates = val
    this.pickerDates.startDate = moment(this.pickerDates.startDate).format('YYYY-MM-DD')
    this.pickerDates.endDate = moment(this.pickerDates.endDate).format('YYYY-MM-DD')
    this.refresh()
  }
  refresh () {
    this.getModelInfo()
    this.getQueryMetrics()
    this.createCharts()
    this.getJobMetrics()
  }
  hideLoading () {
    this.isLoading = false
  }
  numFilter (value) {
    const realVal = parseFloat(value).toFixed(2)
    return realVal
  }
  async getCurrentSettings () {
    this.showLoading()
    try {
      const projectName = this.currentProjectData.name
      const response = await this.fetchProjectSettings({ projectName })
      const result = await handleSuccessAsync(response)
      const quotaSize = result.storage_quota_size / 1024 / 1024 / 1024 / 1024
      this.projectSettings = {...result, storage_quota_tb_size: quotaSize.toFixed(2)}
    } catch (e) {
      handleError(e)
    }
    this.hideLoading()
  }
  getModelInfo () {
    this.getModelStatistics({projectName: this.currentProjectData.name, modelName: null}).then((res) => {
      const data = res.data
      if (data) {
        this.totalModel = data.totalModel
        this.avgExpansionRate = this.numFilter(data.avgModelExpansion)
        this.minExpansionRate = this.numFilter(data.minModelExpansion)
        this.maxExpansionRate = this.numFilter(data.maxModelExpansion)
      }
    })
  }
  getQueryMetrics () {
    this.getQueryStatistics({projectName: this.currentProjectData.name,
      startTime: String(this.pickerDates.startDate),
      endTime: String(this.pickerDates.endDate),
      modelName: null}).then((res) => {
      const data = res.data
      if (data) {
        this.queryCount = data.queryCount
        this.avgQueryLatency = data.avgQueryLatency
      }
    })
  }
  getJobMetrics () {
    this.getJobStatistics({projectName: this.currentProjectData.name,
      startTime: String(this.pickerDates.startDate),
      endTime: String(this.pickerDates.endDate),
      modelName: null}).then((res) => {
      const data = res.data
      if (data) {
        this.jobCount = data.jobCount
        this.jobTotalByteSize = data.jobTotalByteSize
        this.jobTotalLatency = data.jobTotalLatency
        if (this.jobTotalByteSize > 0) {
          this.jobAvgBuildTime = this.numFilter(this.determineAvgJobBuildTimeUnit(
            this.jobTotalLatency / this.jobTotalByteSize))
        } else {
          this.jobAvgBuildTime = 0
        }
      }
    })
  }
  data () {
    const startDate = moment().subtract('days', 5).format('YYYY-MM-DD')
    const endDate = moment().subtract('days', -1).format('YYYY-MM-DD')
    return {
      pickerDates: {
        startDate,
        endDate
      }
    }
  }
  mounted () {
    this.getCurrentSettings()
    this.getModelInfo()
    this.getQueryMetrics()
    this.queryCountChart()
    this.getJobMetrics()
  }
  // create chart
  queryCountChart () {
    this.currentSquare = 'queryCount'
    this.barChartCategory = this.category[0]
    this.barChartMetric = this.metric[0].value
    this.lineChartCategory = this.category[0]
    this.lineChartMetric = this.metric[0].value
    this.createCharts()
  }
  queryAvgChart () {
    this.currentSquare = 'queryAvg'
    this.barChartCategory = this.category[0]
    this.barChartMetric = this.metric[1].value
    this.lineChartCategory = this.category[0]
    this.lineChartMetric = this.metric[1].value
    this.createCharts()
  }
  jobCountChart () {
    this.currentSquare = 'jobCount'
    this.barChartCategory = this.category[1]
    this.barChartMetric = this.metric[2].value
    this.lineChartCategory = this.category[1]
    this.lineChartMetric = this.metric[2].value
    this.createCharts()
  }
  jobAvgBuildChart () {
    this.currentSquare = 'jobAvgBuild'
    this.barChartCategory = this.category[1]
    this.barChartMetric = this.metric[3].value
    this.lineChartCategory = this.category[1]
    this.lineChartMetric = this.metric[3].value
    this.createCharts()
  }
  createCharts () {
    this.createChart(this.barChartDimension, this.barChartCategory, this.barChartMetric, 'bar')
    this.createChart(this.lineChartDimension, this.lineChartCategory, this.lineChartMetric, 'line')
  }
  async createChart (dimension, category, metric, chartType) {
    const startTime = String(this.pickerDates.startDate)
    const endTime = String(this.pickerDates.endDate)
    if (chartType === 'bar') {
      let xdata = []
      let ydata = []
      this.barChartOptions = baseOptions.barChartOptions(xdata, ydata)
      this.barChartOptions.series.type = chartType
      const data = await this.getChartDataResult(category, dimension, metric,
        this.currentProjectData.name, null, startTime, endTime)
      for (let key in data) {
        xdata.push(key)
        if (this.barChartMetric === this.metric[1].value) {
          ydata.push(this.numFilter(data[key] / 1000))
        } else if (this.barChartMetric === this.metric[3].value) {
          ydata.push(this.numFilter(this.determineAvgJobBuildTime(data[key])))
        } else {
          ydata.push(data[key])
        }
      }
      this.barChartOptions.xAxis.data = xdata
      this.barChartOptions.series.data = ydata
      this.barChartOptions.title.text = metric + ' BY ' + dimension
      console.log(this.barChartOptions)
    } else if (chartType === 'line') {
      let xdata = []
      let ydata = []
      this.lineChartOptions = baseOptions.lineChartOptions(xdata, ydata)
      this.lineChartOptions.series.type = chartType
      const data = await this.getChartDataResult(category, dimension, metric,
        this.currentProjectData.name, null, startTime, endTime)
      for (let key in data) {
        xdata.push(key)
        if (this.lineChartMetric === this.metric[1].value) {
          ydata.push(this.numFilter(data[key] / 1000))
        } else if (this.lineChartMetric === this.metric[3].value) {
          ydata.push(this.numFilter(this.determineAvgJobBuildTime(data[key])))
        } else {
          ydata.push(data[key])
        }
      }
      this.lineChartOptions.title.text = metric + ' BY ' + dimension
      this.lineChartOptions.xAxis.data = xdata
      this.lineChartOptions.series.data = ydata
    }
  }
  // if data >= 100 ms/byte then transform display unit sec/MB to min/MB
  determineAvgJobBuildTimeUnit (data) {
    let avgTime = data < 100 ? data * 1024 * 1024 / 1000 : data * 1024 * 1024 / 1000 / 60
    this.jobTimeUnit = data < 100 ? 'sec' : 'min'
    return avgTime
  }
  determineAvgJobBuildTime (data) {
    return this.jobTimeUnit === 'sec' ? data * 1024 * 1024 / 1000 : data * 1024 * 1024 / 1000 / 60
  }
  async getChartDataResult (category, dimension, metric, projectName, modelName, startTime, endTime) {
    const res = await this.getChartData({
      category: category,
      dimension: dimension,
      metric: metric,
      projectName: projectName,
      modelName: modelName,
      startTime: startTime,
      endTime: endTime
    })
    if (res) return res.data
    return null
  }
  // refresh line chart by dimension
  getCurrentSelectedLineChart () {
    let temp = this.currentSelectFilter
    for (let k in this.chartFilter) {
      if (this.chartFilter[k].name === temp) {
        temp = this.chartFilter[k].value
        break
      }
    }
    for (let k in this.dimension) {
      if (temp === this.dimension[k].name) {
        this.lineChartDimension = this.dimension[k].value
        break
      }
    }
    this.createChart(this.lineChartDimension, this.lineChartCategory, this.lineChartMetric, 'line')
  }
  // checkbox display
  showBarChartValue () {
    this.barChartOptions.series[0].label.show = this.barChartOptions.series[0].label.show === false
  }
}
</script>

<style lang="less">
.dashboard {
  height: 100%;
  padding: 20px;
  .dashboard-header {
    margin-bottom: 20px;
  }
  .dashboard-header h1 {
    font-size: 18px;
  }
  .dashboard-body {
    height: 100%;
  }
  .square-big {
    border: 5px solid #ddd;
    text-align: center;
    width: 185px;
    height: 225px;
    padding: 25px 0;
    margin-bottom: 30px;
    .title {
      font-size: 24px;
      height: 55px;
    }
    .metric {
      padding: 15px 0;
      font-size: 50px ;
      font-weight: bolder;
      height: 100px;
      color: #06d;
      .unit {
        font-size: 35px;
      }
    }
    .description {
      font-size: 18px;
      color: #6a6a6a;
    }
  }
  .square {
    border: 2px solid #ddd;
    text-align: center;
    width: 170px;
    height: 165px;
    cursor: zoom-in;
    padding-top: 20px;
    padding-bottom: 15px;
    margin-left: 230px;
    .title {
      font-size: 20px;
      height: 65px;
    }
    .metric {
      font-size: 35px ;
      font-weight: bolder;
      color: #8b1;
      display: inline-block;
      white-space: nowrap;
      .unit {
        font-size: 16px;
      }
    }
    .description {
      font-size: 15px;
      color: #6a6a6a;
      display: block;
    }
  }
  .square-active {
    border: 2px solid #6a6a6a;
  }
  .square1 {
    margin-top: -204px;
    border: 2px solid #ddd;
    text-align: center;
    width: 170px;
    height: 165px;
    cursor: zoom-in;
    padding-top: 20px;
    padding-bottom: 15px;
    margin-left: 440px;
    .title {
      font-size: 20px;
      height: 65px;
    }
    .metric {
      font-size: 35px ;
      font-weight: bolder;
      color: #8b1;
      display: inline-block;
      white-space: nowrap;
      .unit {
        font-size: 16px;
      }
    }
    .description {
      font-size: 15px;
      color: #6a6a6a;
      display: block;
    }
  }
  .square2 {
    margin-top: -204px;
    border: 2px solid #ddd;
    text-align: center;
    width: 170px;
    height: 165px;
    cursor: zoom-in;
    padding-top: 20px;
    padding-bottom: 15px;
    margin-left: 650px;
    .title {
      font-size: 20px;
      height: 65px;
    }
    .metric {
      font-size: 35px ;
      font-weight: bolder;
      color: #8b1;
      display: inline-block;
      white-space: nowrap;
      .unit {
        font-size: 16px;
      }
    }
    .description {
      font-size: 15px;
      color: #6a6a6a;
      display: block;
    }
  }
  .square3 {
    margin-top: -204px;
    border: 2px solid #ddd;
    text-align: center;
    width: 170px;
    height: 165px;
    cursor: zoom-in;
    padding-top: 20px;
    padding-bottom: 15px;
    margin-left: 860px;
    .title {
      font-size: 20px;
      height: 65px;
    }
    .metric {
      font-size: 35px ;
      font-weight: bolder;
      color: #8b1;
      display: inline-block;
      white-space: nowrap;
      .unit {
        font-size: 16px;
      }
    }
    .description {
      font-size: 15px;
      color: #6a6a6a;
      display: block;
    }
  }
  .barChartSquare {
    margin-top: 30px;
    border: 2px solid #ddd;
    text-align: center;
    width: 450px;
    height: 335px;
    cursor: zoom-in;
    padding-top: 10px;
    padding-bottom: 15px;
    margin-left: 230px;
  }
  .lineChartSquare {
    margin-top: -365px;
    border: 2px solid #ddd;
    text-align: right;
    width: 450px;
    height: 360px;
    cursor: zoom-in;
    margin-left: 710px;
  }
  .form-control {
    border-radius: 0px !important;
    box-shadow: none;
    border-color: #5470c6;
    padding-top: -25px;
    padding-bottom: 15px;
    width: 105px;
  }
  .select-control {
    border-radius: 0px !important;
    box-shadow: none;
    border-color: #5470c6;
    width: 80px;
    padding: 4px;
  }
  #datepicker {
    font-family: "Avenir", Helvetica, Arial, sans-serif;
    -webkit-font-smoothing: antialiased;
    -moz-osx-font-smoothing: grayscale;
    text-align: right;
    color: #2c3e50;
    margin-right: 280px;
    height: 24px;
    margin-top: -25px;
  }
}
</style>
