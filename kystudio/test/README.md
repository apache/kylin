#### Jest 安装注意事项

1、babel-jest 的安装注意
  - 如果使用babel7，按如下方式添加babel-jest依赖
  ```
    npm install babel-jest babel-core@^7.0.0-0 @babel/core --save-dev
  ```
  - 如果代码中有使用@修饰符，需要在.babelrc中开启，默认关闭，必须先安装这个插件
  ```
    "plugins": [
      ["@babel/plugin-proposal-decorators", { "legacy": true }]
    ]
  ```
  此项目中使用的是babel6，只需默认安装 ```babel-jest，jest 以及 vue-jest``` 这几个包文件。

2、jest config配置文件注意项
  - moduleNameMapper 别名配置项需要以正则形式进行匹配，如下所示：
    ```
      moduleNameMapper: {
        'vue$': 'vue/dist/vue.common.js',
        '^src': '<rootDir>/src',
        '^assets': '<rootDir>/src/assets',
        '^components': '<rootDir>/src/components',
        '^lessdir': '<rootDir>/src/less',
        '^util': '<rootDir>/src/util',
        '^config': '<rootDir>/src/config'
      }
    ```
  - transformIgnorePatterns 默认忽略 ```node_modules``` 文件，如果引用的模块需要被 babel 解析，按如下方式排除此包
    ```
      "transformIgnorePatterns": [
        "node_modules/(?!(react-native)/)"
      ]
    ```
  - 项目中运用了 eslint 需要配置 ```jest: true``` 属性。
  
3、运行单元测试时错误提示: ```Duplicate declaration "h"```
  - 项目中引入了 ```transform-vue-jsx``` 包，在配置 babel 时，无需重复引用 ```transform-vue-jsx``` 插件。如下：
    ```
      {
        "presets": [
          ["es2015", { "modules": false }],
          "stage-2"
        ],
        "plugins": ["transform-runtime", "transform-vue-jsx", "transform-decorators-legacy"],
        "comments": false,
        "env": {
          "test": {
            "presets": [
              ["es2015", { "targets": { "node": "current" }}],
              "stage-2"
            ],
            "plugins": ["transform-runtime", "transform-decorators-legacy", "dynamic-import-node"]
          }
        }
      }
    ```
  - 更新 ```transform-vue-jsx``` 包为 3.3.0 版本，3.5.0 版本仍然存在一定的缺陷。

