<template>
  <div>
    <div class="login-header">
      <img src="../../assets/img/logo/big_logo.png">
      <ul class="ksd-fright">
        <!-- 在登录页不显示onoff -->
        <li><kylin-help isLogin="login"></kylin-help></li>
        <!-- <li><kylin-change-lang></kylin-change-lang></li> -->
      </ul>
    </div>

    <div class="login-box" v-if="!accessForbidden">
      <el-row :gutter="0">
        <el-col :span="12">
          <div class="grid-content login-msg">
            <img src="../../assets/img/logo/logo_login.png">
            <p>{{$t('welcome')}}</p>
            <p>{{$t('kylinMsg')}}</p>
            <div class="ky-line"></div>
            <ul>
              <li><i class="el-icon-ksd-login_intro ksd-fs-12"></i><a :href="$t('introductionUrl')" target="_blank">{{$t('introduction')}}</a></li>
              <li><i class="el-icon-ksd-details ksd-fs-12"></i><a :href="'https://kylin.apache.org/docs/'" target="_blank">{{$t('manual')}}</a></li>
              <li><i class="el-icon-ksd-login_email ksd-fs-12"></i><a href="mailto:user@Kyligence.io">{{$t('contactUs')}}</a></li>
            </ul>
          </div>
        </el-col>
        <el-col :span="12">
          <div class="login-form">
            <el-form   @keyup.native.enter="onLoginSubmit" :model="user" ref="loginForm" :rules="rules">
              <div class="input_group">
                <el-form-item label="" prop="username">
                  <el-input v-model.trim="user.username" auto-complete="on" :autofocus="true"  :placeholder="$t('userName')" name="username"></el-input>
                </el-form-item>
                <el-form-item label="" prop="password" class="password">
                  <el-input  type="password" v-model="user.password" name="password" :placeholder="$t('password')"></el-input>
                </el-form-item>
                <!-- <p class="forget-pwd ksd-mt-5" v-show="user.username==='ADMIN'">
                  <common-tip :content="$t('adminTip')" >
                    {{$t('forgetPassword')}}
                  </common-tip>
                </p> -->
              </div>
              <el-form-item class="ksd-pt-40">
                <el-button type="primary" class="login-btn ksd-mt-10 kylin-button" :loading="loginBtnLoading"  @keyup.native.enter="onLoginSubmit" @click="onLoginSubmit" ref="loginBtn">{{$t('loginIn')}}</el-button>
              </el-form-item>
            </el-form>
          </div>
        </el-col>
      </el-row>
    </div>
    <div class="accessForbidden-con" v-if="accessForbidden">
      <i class="el-icon-ksd-sad"></i>
      <p class="text">{{$t('accessForbidden')}}</p>
    </div>
    <p class="login-footer">
      <a href="http://kylin.apache.org" style="color:#808080;">Apache Kylin</a> | 
      <a href="http://kylin.apache.org/community/" style="color:#808080;"> Apache Kylin Community</a>
    </p>
  </div>
</template>
<script>
import { mapActions, mapMutations } from 'vuex'
import { handleSuccess, handleError } from '../../util/business'
import changeLang from '../common/change_lang'
import help from '../common/help'
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { Base64 } from 'js-base64'
@Component({
  methods: {
    ...mapActions({
      login: 'LOGIN',
      getPublicConfig: 'GET_CONF'
    }),
    ...mapMutations({
      setCurUser: 'SAVE_CURRENT_LOGIN_USER'
    }),
    onLoginSubmit () {
      this.$refs['loginForm'].validate((valid) => {
        if (valid) {
          var baseStr = 'Basic ' + Base64.encode(this.user.username + ':' + this.user.password)
          Vue.http.headers.common['Authorization'] = baseStr
          this.loginBtnLoading = true
          this.login(this.user).then((res) => {
            handleSuccess(res, (data) => {
              this.loginEnd()
              this.setCurUser({ user: data })
              this.loginSuccess = true
              this.$router.push('/query/insight')
              localStorage.setItem('username', this.user.username)
              localStorage.setItem('loginIn', true)
              this.$store.state.config.overLock = false
            })
          }, (res) => {
            handleError(res)
            this.loginEnd()
          })
        }
      })
    },
    loginEnd () {
      this.loginBtnLoading = false
      Vue.http.headers.common['Authorization'] = ''
    }
  },
  components: {
    'kylin-change-lang': changeLang,
    'kylin-help': help
  },
  computed: {
    rules () {
      return {
        username: [{ required: true, message: this.$t('noUserName'), trigger: 'blur' }],
        password: [{ required: true, message: this.$t('noUserPwd'), trigger: 'blur' }]
      }
    }
  },
  locales: {
    'en': {
      welcome: 'Welcome to Kylin',
      kylinMsg: 'Analytic Data Warehouse for Big Data',
      loginIn: 'Login',
      userName: 'Username',
      password: 'Password',
      forgetPassword: 'Forget Password',
      noUserName: 'Please enter your username.',
      noUserPwd: 'Please enter your password.',
      adminTip: 'Please run the reset password command "bin/admin-tool.sh admin-password-reset" under the "$KYLIN_HOME", <br/>After that, the ADMIN password will be reset to the default password, <br/>and the other accounts will not be influenced.',
      introduction: 'Introduction',
      introductionUrl: 'https://kylin.apache.org/',
      manual: 'Manual',
      contactUs: 'Contact Us',
      accessForbidden: 'Access denied'
    }
  }
})
export default class Login extends Vue {
  data () {
    return {
      accessForbidden: false,
      user: {
        username: '' || localStorage.getItem('username'),
        password: ''
      },
      loginSuccess: false,
      loginBtnLoading: false
    }
  }
}
</script>
<style lang="less">
  @import '../../assets/styles/variables.less';
  .login-header{
    height: 52px;
    background-color: @color-menu-color1;
    ul {
      margin-top: 10px;
      li{
        vertical-align: middle;
        display: inline-block;
        margin-right: 10px;
      }
    }
    img {
      height: 28px;
      z-index: 999;
      margin: 12px 20px;
    }
    .el-dropdown .el-button {
      background-color: transparent;
      border: none;
      font-size: 14px;
      &:hover {
        background-color: transparent;
        color: #fff;
      }
      &.is-plain:not(.is-disabled):hover{
        background-color: transparent;
        color: #fff;
      }
    }
    .el-dropdown i,
    .help-box i{
      color: @fff;
    }
  }
  .login-footer{
    color: #8a8a8a;
    text-align: center;
    bottom: 0px;
    position: fixed;
    width: 100%;
    min-height: 18px;
    padding: 19px 0px;
    background-color: #FFF;
    border-top: 1px solid #d4d6dd;
  }
  .login-box{
    width: 840px;
    border: 1px solid @line-split-color;
    position: absolute;
    background:@fff;
    top: 50%;
    margin-top: -208px;
    left: 50%;
    margin-left: -420px;
    .grid-content {
      text-align: center;
      height: 416px;
    }
    .login-msg {
      border-right: 1px solid @line-split-color;
      img{
        height: 71px;
        margin: 40px 0 31px;
      }
      p:first-of-type {
        color: @text-title-color;
        font-size: 20px;
        margin-bottom: 11px;
        font-weight: @font-medium;
        height: 28px;
      }
      p:last-of-type {
        color: @text-title-color;
        height: 20px;
      }
      .ky-line{
        background: @line-split-color;
        width: 370px;
        margin: 20px 24px 0;
      }
      ul{
        margin: 20px 0 0 140px;
        li {
          list-style: none;
          text-align: left;
          margin-bottom: 2px;
          height: 22px;
          color: @text-normal-color;
          i {
            margin-right: 7px;
          }
          a {
            color: @text-normal-color;
          }
          a:hover {
            color: @base-color-1;
          }
        }        
        li:hover {
          color: @base-color-1;
        }
      }
    }
    .login-form {
      .el-form {
        width: 320px;
        padding: 116px 50px 0px;
      }
      .forget-pwd{
        text-align: right;
        // margin-top: -10px;
        font-size: 12px;
        color: @text-disabled-color;
        height: 16px;
        cursor: pointer;
      }
      .forget-pwd:hover{
        color:@base-color-2;
        text-decoration: underline;
      }
      .el-button {
        width: 320px;
      }
    }
  }
  .accessForbidden-con {
    position: absolute;
    top: 30%;
    left: 50%;
    transform: translate(-50%, -30%);
    text-align: center;
    .el-icon-ksd-sad {
      font-size: 40px;
    }
    .text {
      margin-top: 17px;
      font-size: 22px;
      color: @text-title-color;
    }
  }
</style>
