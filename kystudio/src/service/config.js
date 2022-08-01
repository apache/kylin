import Vue from 'vue'
import VueResource from 'vue-resource'
import { apiUrl } from '../config'

Vue.use(VueResource)

export default {
  getDefaults: (type) => {
    return Vue.resource(apiUrl + 'config/defaults/' + type).get()
  },
  hiddenMeasure: (feature) => {
    return Vue.resource(apiUrl + 'config/hidden_feature').get(feature)
  },
  isCloud: () => {
    return Vue.resource(apiUrl + 'config/is_cloud').get()
  }
}
