const fs = require('fs')
const path = require('path')
const dotenv = require('dotenv')

const e2ePath = path.resolve('./specs/e2e.env')
const e2ePathLocal = path.resolve('./specs/e2e.env.local')

if (fs.existsSync(e2ePath)) {
  dotenv.config({ path: e2ePath })
}

if (fs.existsSync(e2ePathLocal)) {
  dotenv.config({ path: e2ePathLocal })
}

// require('./systemAdmin/login/login.spec')
// require('./systemAdmin/logout/logout.spec')
// 在系统 admin 的 case 中建项目、建模型、建项目的 4 个角色的用户
require('./systemAdmin/happy_path/index.spec')
// 分别用上一个 case 创建的角色进行登录
// require('./projectAdmin/happy_path/index.spec')
// require('./projectManagement/happy_path/index.spec')
// require('./projectOperation/happy_path/index.spec')
// require('./projectQuery/happy_path/index.spec')
// 删除前面几个 case 中创建的 用户、模型、项目
// require('./clear/index.spec')
