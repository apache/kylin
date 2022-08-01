// see http://vuejs-templates.github.io/webpack for documentation.
var path = require('path')
// var cmdArg = process.argv.splice(2) && process.argv.splice(2)[0] || ''
var proxyTable = {}
var argvs = process.argv.slice(2)
let port = argvs[1] || 8080
console.log(port)
if (argvs && argvs.indexOf('proxy') !== -1) {
  var proxyBase = 'http://localhost:' + port
  var proxyHost = 'http://localhost:7070' // 测试其他环境的数据请修改该地址进行转发
  proxyTable = {
    '/kylin/api': {
      target: proxyHost,
      pathRewrite: {
        '^/kylin': '/kylin'
      }
    },
    // 后续暂时用不到
    '/kylin/j_spring_security_logout': {
      target: proxyBase,
      pathRewrite: {
        '^/kylin': '#/kylin'
      }
    }
  }
}
module.exports = {
  build: {
    env: require('./prod.env'),
    index: path.resolve(__dirname, '../dist/index.html'),
    assetsRoot: path.resolve(__dirname, '../dist'),
    assetsSubDirectory: 'static',
    assetsPublicPath: '/kylin/',
    productionSourceMap: false,
    // Gzip off by default as many popular static hosts such as
    // Surge or Netlify already gzip all static assets for you.
    // Before setting to `true`, make sure to:
    // npm install --save-dev compression-webpack-plugin
    productionGzip: false,
    productionGzipExtensions: ['js', 'css'],
    // Run the build command with an extra argument to
    // View the bundle analyzer report after build finishes:
    // `npm run build --report`
    // Set to `true` or `false` to always turn it on or off
    bundleAnalyzerReport: process.env.npm_config_report
  },
  dev: {
    env: require('./dev.env'),
    port: port,
    autoOpenBrowser: true,
    assetsSubDirectory: 'static',
    assetsPublicPath: '/',
    proxyTable: proxyTable,
    // CSS Sourcemaps off by default because relative paths are "buggy"
    // with this option, according to the CSS-Loader README
    // (https://github.com/webpack/css-loader#sourcemaps)
    // In our experience, they generally work as expected,
    // just be aware of this issue when enabling this option.
    cssSourceMap: false
  }
}
