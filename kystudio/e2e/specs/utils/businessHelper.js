const { until, By, Key } = require('selenium-webdriver')
const { waitingForStable, clearFormInput, changeFormInput, changeFormSelect, dragAndDrop } = require('../utils/domHelper')

exports.closeLicenseBox = async function closeLicenseBox (driver) {
  try {
    await driver.findElement(By.css('.el-dialog__wrapper.linsencebox')).click()
  } catch (e) {}
}

exports.waitingForPageClean = async function waitingForPageClean (driver) {
  try {
    const messageBoxWrappers = await driver.findElements(By.css('.el-message-box__wrapper'))
    for (const messageBoxWrapper of messageBoxWrappers) {
      try {
        await driver.wait(until.elementIsNotVisible(messageBoxWrapper), 10000)
      } catch (e) {}
    }
  } catch (e) {}

  try {
    const messageBoxWrappers = await driver.findElements(By.css('.el-dialog__wrapper'))
    for (const messageBoxWrapper of messageBoxWrappers) {
      try {
        await driver.wait(until.elementIsNotVisible(messageBoxWrapper), 10000)
      } catch (e) {}
    }
  } catch (e) {}
}

// 封装的登录
exports.login = async function login(driver, username, password) {
  await driver.wait(until.elementLocated(By.css('.login-form .el-button--primary')), 10000)
  await driver.findElement(By.css('.ke-it-cn')).click()
  // 浏览器会自动填入 admin，所以要先置空用户名的输入框
  await clearFormInput(driver, '.login-form .input_group .el-form-item:nth-child(1) input')
  await driver.sleep(2000)
  await driver.findElement(By.css('.login-form .input_group .el-form-item:nth-child(1) input')).sendKeys(username)
  await driver.findElement(By.css('.login-form .input_group .el-form-item:nth-child(2) input')).sendKeys(password)
  await driver.findElement(By.css('.login-form .el-button--primary')).click()

  await driver.wait(until.elementLocated(By.css('.topbar .limit-user-name')), 10000)
}

// 封装的登出
exports.logout = async function logout(driver) {
  const usernameEl = await driver.findElement(By.css('.topbar .user-msg-dropdown .el-dropdown-link'))
  const dropdownMenuId = await usernameEl.getAttribute('aria-controls')
  await driver.actions().move({ origin: usernameEl }).perform()
  await driver.sleep(1000)

  await driver.wait(until.elementLocated(By.id(dropdownMenuId)), 10000)
  await driver.findElement(By.css(`ul#${dropdownMenuId} .el-dropdown-menu__item:nth-child(3)`)).click()

  await driver.wait(until.elementLocated(By.css('.el-message-box')), 10000)
  await driver.findElement(By.css(`.el-message-box .el-button--primary`)).click()

  await driver.wait(until.elementLocated(By.css('.login-form')), 10000)
}

exports.addUser = async function addUser (driver, username, pwd) {
  await driver.sleep(2000)
  await driver.findElement(By.css('.security-user .el-row .el-button--primary')).click()
  await driver.sleep(1000)

  changeFormInput(driver, '.user-edit-modal .js_username', username)
  await driver.sleep(1000)
  changeFormInput(driver, '.user-edit-modal .js_password', pwd)
  await driver.sleep(1000)
  changeFormInput(driver, '.user-edit-modal .js_confirmPwd', pwd)
  await driver.sleep(1000)
  await driver.findElement(By.css('.user-edit-modal .el-dialog__footer .el-button--primary')).click()
  await driver.sleep(5000)
}

exports.delUser = async function delUser (driver, username, idx) {
  // 先清空搜索
  await clearFormInput(driver, '.show-search-btn input')
  await driver.sleep(1000)

  // 精确搜索想要删除的用户，保证列表只有一条记录
  await changeFormInput(driver, '.show-search-btn', username)
  await driver.sleep(1000)

  const actions = driver.actions({bridge: true})
  // 执行回车搜索
  await actions.click(await driver.findElement(By.css('.show-search-btn input'))).sendKeys(Key.ENTER).perform()

  await driver.sleep(2000)
  // 点击右侧更多按钮
  await driver.findElement(By.css('.el-icon-ksd-table_others')).click()
  let moreBtnEl = await driver.findElement(By.css('.el-icon-ksd-table_others'))
  // 更多按钮上的 aria-controls 属性对应的就是 dropdown 的下拉 div 的id
  let dropdownMenuId = await moreBtnEl.getAttribute('aria-controls')
  
  await driver.actions().move({ origin: moreBtnEl }).perform()
  await driver.sleep(1000)

  await driver.wait(until.elementLocated(By.id(dropdownMenuId)), 10000)
  await driver.findElement(By.css(`ul#${dropdownMenuId} .el-dropdown-menu__item:nth-child(${idx})`)).click()

  await driver.wait(until.elementIsVisible(driver.findElement(By.css('div[aria-label=删除用户] .el-message-box__btns .el-button--primary'))), 10000)
  await driver.findElement(By.css('div[aria-label=删除用户] .el-message-box__btns .el-button--primary')).click()
  await driver.sleep(2000)
}

exports.searchCurProject = async function searchCurProject (driver, projectname) {
  await changeFormInput(driver, '#project-list .show-search-btn', projectname)
  await driver.sleep(1000)

  const actions = driver.actions({bridge: true})
  // 执行回车搜索
  await actions.click(await driver.findElement(By.css('#project-list .show-search-btn input'))).sendKeys(Key.ENTER).perform()
  await driver.sleep(2000)
}

exports.setUserToProject = async function setUserToProject (driver, username, lineIdx, typeIdx) {
  // 选 principal
  const userSel = `.author_dialog .user-group-select:nth-child(${lineIdx}) .user-select`
  await changeFormSelect(driver, userSel, `.js_principal${lineIdx - 2}`, 1)
  await driver.sleep(1000)

  // 选具体的人，需要先搜这个人，然后匹配出下拉第几位
  const user = `.author_dialog .user-group-select:nth-child(${lineIdx}) .name-select${lineIdx - 2}`
  await changeFormInput(driver, user, username)
  await driver.sleep(1000)
  let userIdx = 0
  let userList = await driver.findElements(By.css(`.js_author-select${lineIdx - 2} .el-select-dropdown__item`))
  for (let i = 0; i < userList.length; i++) {
    let item = userList[i]
    let text = await item.getText()
    if (text === username) {
      userIdx = i
      break
    }
  }
  await changeFormSelect(driver, user, `.js_author-select${lineIdx - 2}`, userIdx + 1, true)
  await driver.sleep(1000)

  // 选具体的权限
  const typeSel = `.author_dialog .user-group-select:nth-child(${lineIdx}) .type-select${lineIdx - 2}`
  await changeFormSelect(driver, typeSel, `.js_access_type_sel${lineIdx - 2}`, typeIdx)
  await driver.sleep(1000)
}

exports.changeToCurProject = async function changeToCurProject (driver, projectName) {
  const projectSelect = '.topbar .project_select'
  // await clearFormInput(driver, `${projectSelect} input`)
  // await changeFormInput(driver, projectSelect, projectName)
  await driver.findElement(By.css(`${projectSelect} .el-input`)).click()
  await driver.sleep(1000)
  let projectIdx = 0
  let projectList = await driver.findElements(By.css(`.project-select_dropdown .el-select-dropdown__item`))
  for (let i = 0; i < projectList.length; i++) {
    let item = projectList[i]
    let text = await item.getText()
    if (text === projectName) {
      projectIdx = i
      break
    }
  }
  await changeFormSelect(driver, projectSelect, '.project-select_dropdown', projectIdx + 1, true)
  await driver.sleep(1000)
}

exports.addTableLink = async function addTableLink (driver, fTable, pTable, fColumnIdx, pColumnIdx) {
  await dragAndDrop(driver, `.model-edit-outer ${fTable} .column-list-box ul li:nth-child(1)`, `.model-edit-outer ${pTable} .column-list-box`)
  await driver.wait(until.elementIsVisible(driver.findElement(By.css('.links-dialog'))))
  // 选择SSB.P_LINEORDER LEFT join SSB.DATES, LO_ORDERDATE = D_DATEKEY
  await changeFormSelect(driver, '.links-dialog .link-type', '.js_link-type .el-select-dropdown__list', 2)
  await changeFormSelect(driver, '.links-dialog .foreign-select', '.js_foreign-select .el-select-dropdown__list', fColumnIdx)
  await changeFormSelect(driver, '.links-dialog .join-type', '.js_join-type .el-select-dropdown__list', 1)
  await changeFormSelect(driver, '.links-dialog .primary-select', '.js_primary-select .el-select-dropdown__list', pColumnIdx)
  await driver.findElement(By.css('.links-dialog .el-dialog__footer .el-button:nth-child(2)')).click()
  await driver.sleep(1000)
}

exports.addMeasure = async function addMeasure (driver, measureName, expression, parameter) {
  await driver.findElement(By.css('.model-edit-outer .panel-measure .el-icon-ksd-project_add')).click()
  await driver.wait(until.elementIsVisible(driver.findElement(By.css('.add-measure-modal'))))
  await changeFormInput(driver, '.measure-name-input', measureName)

  const expressionSelect = '.add-measure-modal .measure-expression-select'
  await driver.findElement(By.css(expressionSelect + ' .el-input')).click()
  await driver.sleep(1000)
  let expressionIdx = 0
  let expressionList = await driver.findElements(By.css(`.js_measure-expression .el-select-dropdown__item`))
  for (let i = 0; i < expressionList.length; i++) {
    let item = expressionList[i]
    let text = await item.getText()
    if (text === expression) {
      expressionIdx = i
      break
    }
  }
  await changeFormSelect(driver, expressionSelect, '.js_measure-expression', expressionIdx + 1, true)
  await driver.sleep(1000)

  const parameterSelect = '.add-measure-modal .parameter-select'
  await driver.findElement(By.css(parameterSelect + ' .el-input')).click()
  await driver.sleep(1000)
  let parameterIdx = 0
  let parameterList = await driver.findElements(By.css(`.js_parameter-select .el-select-dropdown__item`))
  for (let i = 0; i < parameterList.length; i++) {
    let item = parameterList[i]
    let text = await item.getText()
    if (text.indexOf(parameter) !== -1) {
      parameterIdx = i
      break
    }
  }
  await changeFormSelect(driver, parameterSelect, '.js_parameter-select', parameterIdx + 1, true)
  await driver.sleep(1000)

  await driver.findElement(By.css('.add-measure-modal  .el-dialog__footer .el-button:nth-child(2)')).click()
  await driver.wait(until.elementIsNotVisible(driver.findElement(By.css('.add-measure-modal '))))
}