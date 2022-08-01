import Vue from 'vue'
import VueResource from 'vue-resource'
import { apiUrl } from '../config'

Vue.use(VueResource)

export default {
  getUsersList: (para) => {
    return Vue.resource(apiUrl + 'user').get(para)
  },
  updateStatus: (user) => {
    return Vue.resource(apiUrl + 'user').update(user)
  },
  saveUser: (user) => {
    return Vue.resource(apiUrl + 'user').save(user.detail)
  },
  editRole: (user) => {
    return Vue.resource(apiUrl + 'user').update(user)
  },
  resetPassword: (user) => {
    return Vue.resource(apiUrl + 'user/password').update(user)
  },
  removeUser: (uuid) => {
    return Vue.resource(apiUrl + 'user/' + uuid).remove()
  },
  // access
  login: () => {
    return Vue.resource(apiUrl + 'user/authentication').save()
  },
  loginOut: () => {
    return Vue.resource(apiUrl + 'j_spring_security_logout').get()
  },
  authentication: () => {
    return Vue.resource(apiUrl + 'user/authentication').get()
  },
  userAccess: (para) => {
    return Vue.resource(apiUrl + 'access/permission/project_permission').get(para)
  },
  // user goup
  addGroupsToUser: (para) => {
    return Vue.resource(apiUrl + 'user').update(para)
  },
  addUsersToGroup: (para) => {
    return Vue.resource(apiUrl + 'user_group/users').update(para)
  },
  getUserGroupList: (para) => {
    return Vue.resource(apiUrl + 'user_group/users_with_group').get(para)
  },
  getGroupList: (para) => {
    return Vue.resource(apiUrl + 'user_group/groups').get()
  },
  addGroup: (para) => {
    return Vue.resource(apiUrl + 'user_group').save(para)
  },
  delGroup: (para) => {
    return Vue.resource(apiUrl + 'user_group/' + para.group_uuid).remove()
  },
  getUsersByGroupName: (para) => {
    return Vue.resource(apiUrl + 'user_group/group_members/' + para.group_uuid).get(para)
  },
  getAccessDetailsByUser: (projectName, roleOrName, data, type) => {
    return Vue.resource(apiUrl + `acl/${type}/${roleOrName}`).get(data)
  }
}
