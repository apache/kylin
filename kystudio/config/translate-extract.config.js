module.exports = {
  bundles: {
    查询: /\/query\//,
    登录: [
      /login\.vue/,
    ],
    建模中心: [
      /\/studio\//,
      /\/Model/,
      /\/datasource\//,
      /\/DataSourceModal\//
    ],
    设置: /\/setting\//,
    系统管理: [
      /\/admin\//,
      /\/project\//,
      /\/security\//,
      /\/user\//,
    ],
    其他: '*'
  },
};
