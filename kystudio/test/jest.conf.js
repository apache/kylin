const path = require('path')

module.exports = {
  verbose: false,
  testURL: 'http://localhost/',
  rootDir: path.resolve(__dirname, '../'),
  moduleFileExtensions: [
    'js',
    'json',
    'vue'
  ],
  roots: [
    '<rootDir>/src/'
  ],
  transform: {
    '^.+\\.(vue)$': '<rootDir>/node_modules/vue-jest',
    '^.+\\.jsx?$': '<rootDir>/node_modules/babel-jest',
    '^.+\\.(css|styl|less|sass|scss|svg|png|jpg|ttf|woff|woff2)$': 'jest-transform-stub'
  },
  transformIgnorePatterns: ['node_modules'],
  // 所需忽略的文件
  testPathIgnorePatterns: ['<rootDir>/src/config'],
  testMatch: [
    '<rootDir>/src/**/__test__/**/*.spec.js'
    // '<rootDir>/src/components/common/__test__/DataSourceModal/index.spec.js'
  ],
  setupFiles: ['<rootDir>/test/setup'],
  moduleNameMapper: {
    '^vue$': 'vue/dist/vue.common.js',
    '^src': '<rootDir>/src',
    '^assets': '<rootDir>/src/assets',
    'components(.*)$': '<rootDir>/src/components/$1',
    // '^components': '<rootDir>/src/components',
    '^lessdir': '<rootDir>/src/less',
    '^util': '<rootDir>/src/util',
    '^config': '<rootDir>/src/config'
    // '^themescss': '<rootDir>/node_modules/kyligence-kylin-ui/packages/theme-chalk/src',
    // '^kyligence-kylin-ui': '<rootDir>/node_modules/kyligence-kylin-ui'
  },
  // 遇到有出错的测试用例就不再执行下去
  bail: true,
  collectCoverage: true,
  collectCoverageFrom: [
    'src/canvas/*.{js,vue}',
    'src/filter/*.{js,vue}',
    'src/directive/*.{js,vue}',
    'src/util/*.{js,vue}',
    'src/components/**/*.{js,vue}',
    'src/components/common/__test__/slider.spec.js',
    '!src/**/__test__/**/*.js',
    '!src/**/locales.js',
    '!src/components/dome.vue',
    '!**/node_modules/**',
    '!src/router/**',
    '!src/locale/**',
    '!src/config/**',
    '!src/assets/**',
    '!src/service/**',
    '!src/store/**',
    '!src/util/code_flower.js',
    '!src/util/liquidFillGauge.js',
    '!src/components/common/DemoModal/*.{js,vue}',
    '!src/components/common/FlowerChart/*.{js,vue}',
    '!src/components/datasource/access_sub/*.{js,vue}',
    '!src/components/kafka/*.{js,vue}',
    '!src/components/demo.vue',
    '!src/components/common/DataSourceModal/SourceCSV/**/*.{js,vue}',
    '!src/components/common/BatchLoadModal/*.{js,vue}',
    '!src/components/common/Guide/*.{js,vue}',
    '!src/components/common/fake_progress.vue',
    '!src/components/common/confirm_btn.vue',
    '!src/components/common/date_picker.vue',
    '!src/components/common/icon_button.vue',
    '!src/components/common/manage_card.vue',
    '!src/components/common/kylin_collapse.vue',
    '!src/components/common/common_popover.vue',
    '!src/components/monitor/admin.vue',
    '!src/components/monitor/cluster.vue'
  ],
  // coverageReporters: ['json', 'lcovonly', 'text', 'clover'],
  coverageDirectory: './test/coverage',
  // 自定义覆盖率标准
  coverageThreshold: {
    'global': {
      'branches': 0,
      'functions': 0,
      'lines': 0,
      'statements': 0
    }
  },
  forceCoverageMatch: ['src/**/__test__/**/*.spec.js']
}
