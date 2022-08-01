const { Builder, By, until, Key } = require('selenium-webdriver')
const { login, logout, addTableLink, addMeasure } = require('../../utils/businessHelper')
const { changeFormInput, changeFormTextarea, changeFormSelect, dragAndDrop } = require('../../utils/domHelper')

const {
  BROWSER_ENV,
  LAUNCH_URL,
  USERNAME_ADMIN,
  PASSWORD_ADMIN,
  PROJECT_NAME,
  MODEL_NAME
} = process.env

/* eslint-disable newline-per-chained-call */
describe('系统管理员创建项目', async function () {
  this.timeout(60000)
  let driver

  before(async () => {
    driver = await new Builder().forBrowser(BROWSER_ENV).build()
    await driver.get(LAUNCH_URL)
    await driver.manage().window().setRect({ width: 1580, height: 828 })
  })

  after(async () => {
    await driver.quit()
  })

  // 修改默认密码
  // it('修改默认密码', async () => {
  //   // 统一调用登录
  //   await login(driver, USERNAME_ADMIN, PASSWORD_ADMIN);
  //   await driver.sleep(3000)

  //   try {
  //     await driver.wait(until.elementIsVisible(driver.findElement(By.css('.user-edit-modal'))), 10000)
  //   } catch (e){
  //     return true
  //   }
  //   // 修改密码的弹窗
  //   changeFormInput(driver, '.user-edit-modal .el-dialog__body .js_oldPassword', PASSWORD_ADMIN)
  //   changeFormInput(driver, '.user-edit-modal .el-dialog__body .js_newPassword', PASSWORD_ADMIN_NEW)
  //   changeFormInput(driver, '.user-edit-modal .el-dialog__body .js_confirmPwd', PASSWORD_ADMIN_NEW)
  //   await driver.sleep(1000)
  //   await driver.findElement(By.css('.user-edit-modal .el-dialog__footer .el-button--primary')).click()
  //   await driver.sleep(4000)
  // })

  // it('登出，让修改的密码生效', async () => {
  //   // 执行登出，让修改后的账密状态更新
  //   await logout(driver)
  //   await driver.sleep(2000)
  // })

  // 创建项目
  it('创建项目', async () => {
    // 统一调用登录
    await login(driver, USERNAME_ADMIN, PASSWORD_ADMIN)
    try {
      await driver.findElement(By.css('.linsencebox .el-dialog__footer .el-button:nth-child(2)')).click() // 防止有过期弹窗出现时，第一次点不不到添加项目按钮
      await driver.sleep(1000)
    } catch (e) {}
    await driver.wait(until.elementLocated(By.css('.topbar .add-project-btn')), 3000)
    await driver.sleep(1000)
    await driver.findElement(By.css('.topbar .add-project-btn')).click()

    await driver.wait(until.elementIsVisible(driver.findElement(By.css('.project-edit-modal'))), 10000)
    changeFormInput(driver, '.project-edit-modal .js_projectname', PROJECT_NAME)
    changeFormTextarea(driver, '.project-edit-modal .js_project_desc', 'this is it test auto create project')
    await driver.sleep(1000)
    await driver.findElement(By.css('.project-edit-modal .el-dialog__footer .js_addproject_submit')).click()

    // 接口有时候是 3 秒等待
    await driver.sleep(3000)
    // 添加完成后，进入的是数据源页面
    // assert.equal(await driver.getCurrentUrl(), `${LAUNCH_URL}/#/studio/source`);
    // await driver.sleep(2000)
  })

  // 加载数据源
  it('加载数据源', async () => {
    await driver.wait(until.elementIsVisible(driver.findElement(By.css('.data-source-bar .btn-group .el-button--primary'))), 10000)
    await driver.findElement(By.css('.data-source-bar .btn-group .el-button--primary')).click()
    // until 弹窗出来后 点击 hive
    await driver.wait(until.elementIsVisible(driver.findElement(By.css('.data-srouce-modal'))), 10000)
    await driver.findElement(By.css('.data-srouce-modal .source-new .datasouce .el-icon-ksd-hive')).click()
    await driver.sleep(1000)

    // 点击下一步按钮
    await driver.findElement(By.css('.data-srouce-modal .el-dialog__footer .el-button--primary')).click()
    await driver.sleep(3000)
    // 搜索 ssb
    // await changeFormInput(driver, '.source-hive .list .el-input', 'SSB')
    // const actions = driver.actions({bridge: true})
    // await actions.click(await driver.findElement(By.css('.source-hive .list .el-input'))).sendKeys(Key.ENTER).perform()
    // await driver.sleep(1000)
    // 找到 ssb，点击选中所有
    // await hoverOn(driver, By.css('.guide-ssb'))
    // await driver.findElement(By.css('.guide-ssb .select-all')).click()
    // 收起default(里面有SSB表)展开 ssb
    // try {
    //   await driver.findElement(By.css('.guide-default')).click()
    //   await driver.sleep(1000)
    // } catch (e) {}
    // await driver.findElement(By.css('.guide-ssb')).click()
    // await driver.sleep(1000)
    // await driver.findElement(By.css('.ssb-more')).click()
    // await driver.sleep(1000)
    // 先搜索,再选中table SSB['CUSTOMER', 'DATES', 'PART', 'P_LINEORDER', 'SUPPLIER', 'LINEORDER']
    const actions = driver.actions({bridge: true})
    await changeFormInput(driver, '.source-hive .list .el-input', 'SSB.CUSTOMER')
    await actions.click(await driver.findElement(By.css('.source-hive .list .el-input'))).sendKeys(Key.ENTER).perform()
    await driver.sleep(1000)
    await driver.findElement(By.id('table-load-SSB.CUSTOMER')).click()
    await driver.sleep(1000)
    await changeFormInput(driver, '.source-hive .list .el-input', 'SSB.DATES')
    await actions.click(await driver.findElement(By.css('.source-hive .list .el-input'))).sendKeys(Key.ENTER).perform()
    await driver.sleep(1000)
    await driver.findElement(By.id('table-load-SSB.DATES')).click()
    await driver.sleep(1000)
    await changeFormInput(driver, '.source-hive .list .el-input', 'SSB.PART')
    await actions.click(await driver.findElement(By.css('.source-hive .list .el-input'))).sendKeys(Key.ENTER).perform()
    await driver.sleep(1000)
    await driver.findElement(By.id('table-load-SSB.PART')).click()
    await driver.sleep(1000)
    await changeFormInput(driver, '.source-hive .list .el-input', 'SSB.P_LINEORDER')
    await actions.click(await driver.findElement(By.css('.source-hive .list .el-input'))).sendKeys(Key.ENTER).perform()
    await driver.sleep(1000)
    await driver.findElement(By.id('table-load-SSB.P_LINEORDER')).click()
    await driver.sleep(1000)
    await changeFormInput(driver, '.source-hive .list .el-input', 'SSB.LINEORDER')
    await actions.click(await driver.findElement(By.css('.source-hive .list .el-input'))).sendKeys(Key.ENTER).perform()
    await driver.sleep(1000)
    await driver.findElement(By.id('table-load-SSB.LINEORDER')).click()
    await driver.sleep(1000)
    await changeFormInput(driver, '.source-hive .list .el-input', 'SSB.SUPPLIER')
    await actions.click(await driver.findElement(By.css('.source-hive .list .el-input'))).sendKeys(Key.ENTER).perform()
    await driver.sleep(1000)
    await driver.findElement(By.id('table-load-SSB.SUPPLIER')).click()
    await driver.sleep(1000)
    await driver.findElement(By.css('.data-srouce-modal .el-dialog__footer .el-button--primary')).click()
    await driver.sleep(3000)
    await driver.wait(until.elementIsNotVisible(driver.findElement(By.css('.data-srouce-modal'))))
  })

  // 设置默认数据库
  it('配置默认数据库', async () => {
    // 点击菜单设置
    await driver.wait(until.elementIsNotVisible(driver.findElement(By.css('.data-srouce-modal'))), 20000)
    await driver.findElement(By.css('.el-menu-item .el-icon-ksd-setting')).click()

    // 点击高级设置的 tab
    await driver.wait(until.elementLocated(By.id('tab-advanceSetting')), 3000)
    await driver.findElement(By.id('tab-advanceSetting')).click()

    // 选择 下拉里的 ssb
    await driver.wait(until.elementLocated(By.css('.js_defautDB_block')), 3000)
    await changeFormSelect(driver, '.js_defautDB_block .js_select', '.js_defautDB_select .el-select-dropdown__list', 2)
    await driver.sleep(1000)
    // 点击提交
    await driver.findElement(By.css('#pane-advanceSetting .js_defautDB_block .block-foot .el-button--default')).click()
    // 点击二次确认弹窗的按钮
    await driver.wait(until.elementIsVisible(driver.findElement(By.css('div[aria-label=修改默认数据库]'))), 10000)
    await driver.findElement(By.css('div[aria-label=修改默认数据库] .el-message-box__btns .el-button--primary')).click()
    await driver.sleep(3000)
  })
  // 建模
  it('在项目 PROJECT_NAME 下建模', async () => {
    // 回到dashboard页面
    // await driver.findElement(By.css('.entry-admin')).click()
    // await driver.sleep(3000)
    // 切换到 PROJECT_NAME 项目下
    // await changeToCurProject(driver, PROJECT_NAME)
    // await driver.sleep(2000)

    // 点击跳转到模型页面
    await driver.wait(until.elementLocated(By.id('studio')), 10000)
    await driver.findElement(By.id('studio')).click()
    await driver.sleep(1000)
    await driver.findElement(By.id('studio')).findElement(By.css('.el-menu .el-menu-item:nth-child(2)')).click()
    await driver.sleep(2000)

    // 新建模型
    await driver.wait(until.elementLocated(By.id('addModel')), 10000)
    await driver.findElement(By.id('addModel')).findElement(By.css('.el-button:nth-child(1)')).click()
    await driver.wait(until.elementIsVisible(driver.findElement(By.css('.js_add-model'))))
    await changeFormInput(driver, '.js_add-model .el-form-item:nth-child(1) .el-input', MODEL_NAME)
    await driver.sleep(1000)
    await driver.findElement(By.css('.js_add-model .el-dialog__footer .el-button:nth-child(2)')).click()
    await driver.sleep(2000)
    // 如果有引导遮罩，点击关闭
    let mask = null
    try {
      mask = await driver.findElement(By.css('.model-guide-mask'))
    } catch (e) {}
    if (mask) {
      await driver.findElement(By.css('.model-guide-mask .dim-meas-block .el-button')).click()
      await driver.sleep(1000)
    }

    // 拖入事实表
    await dragAndDrop(driver, '.model-edit-outer .el-tree .guide-ssbp_lineorder', '.model-edit-outer', 800, 200)
    await driver.sleep(1000)
    // 切换事实表
    await driver.findElement(By.css('.model-edit-outer .table-box .el-icon-ksd-table_setting')).click()
    await driver.sleep(1000)
    await driver.findElement(By.css('.model-edit-outer .fast-action-box .switch')).click()
    await driver.sleep(1000)

    // 拖入lookup 表
    await dragAndDrop(driver, '.model-edit-outer .el-tree .guide-ssbdates', '.model-edit-outer', 400, 519)
    await driver.sleep(1000)
    await dragAndDrop(driver, '.model-edit-outer .el-tree .guide-ssbcustomer', '.model-edit-outer', 650, 519)
    await driver.sleep(1000)
    await dragAndDrop(driver, '.model-edit-outer .el-tree .guide-ssbsupplier', '.model-edit-outer', 900, 519)
    await driver.sleep(1000)
    await dragAndDrop(driver, '.model-edit-outer .el-tree .guide-ssbpart', '.model-edit-outer', 1150, 519)
    await driver.sleep(1000)

    // 连接表关系
    await addTableLink(driver, '.js_p_lineorder', '.js_dates', 6, 1)
    await addTableLink(driver, '.js_p_lineorder', '.js_customer', 3, 1)
    await addTableLink(driver, '.js_p_lineorder', '.js_supplier', 5, 1)
    await addTableLink(driver, '.js_p_lineorder', '.js_part', 4, 1)

    // 批量选维度
    await driver.findElement(By.css('.model-edit-outer .panel-dimension .el-icon-ksd-backup')).click()
    await driver.wait(until.elementIsVisible(driver.findElement(By.css('.dimension-modal'))))
    // 添加维度
    // 'P_LINEORDER.LO_ORDERDATE',
    // 'P_LINEORDER.LO_CUSTKEY',
    // 'P_LINEORDER.LO_SUPPKEY',
    // 'P_LINEORDER.LO_PARTKEY',
    await driver.findElement(By.css('.dimension-modal .ksd-mb-10:nth-child(1) .table-header')).click()
    await driver.sleep(1000)
    await driver.findElement(By.css('.dimension-modal .ksd-mb-10:nth-child(1) .el-table .guide-P_LINEORDERLO_ORDERDATE .el-checkbox')).click()
    await driver.sleep(500)
    await driver.findElement(By.css('.dimension-modal .ksd-mb-10:nth-child(1) .el-table .guide-P_LINEORDERLO_CUSTKEY .el-checkbox')).click()
    await driver.sleep(500)
    await driver.findElement(By.css('.dimension-modal .ksd-mb-10:nth-child(1) .el-table .guide-P_LINEORDERLO_SUPPKEY .el-checkbox')).click()
    await driver.sleep(500)
    await driver.findElement(By.css('.dimension-modal .ksd-mb-10:nth-child(1) .el-table .guide-P_LINEORDERLO_PARTKEY .el-checkbox')).click()
    await driver.sleep(500)
    await driver.findElement(By.css('.dimension-modal .ksd-mb-10:nth-child(1) .table-header')).click()
    await driver.sleep(1000)
    // 'DATES.D_DATEKEY',
    await driver.findElement(By.css('.dimension-modal .ksd-mb-10:nth-child(2) .table-header')).click()
    await driver.sleep(1000)
    await driver.findElement(By.css('.dimension-modal .ksd-mb-10:nth-child(2) .el-table .guide-DATESD_DATEKEY .el-checkbox')).click()
    await driver.sleep(500)
    await driver.findElement(By.css('.dimension-modal .ksd-mb-10:nth-child(2) .table-header')).click()
    await driver.sleep(1000)
    // 'CUSTOMER.C_CUSTKEY',
    await driver.findElement(By.css('.dimension-modal .ksd-mb-10:nth-child(3) .table-header')).click()
    await driver.sleep(1000)
    await driver.findElement(By.css('.dimension-modal .ksd-mb-10:nth-child(3) .el-table .guide-CUSTOMERC_CUSTKEY .el-checkbox')).click()
    await driver.sleep(500)
    await driver.findElement(By.css('.dimension-modal .ksd-mb-10:nth-child(3) .table-header')).click()
    await driver.sleep(1000)
    // 'SUPPLIER.S_SUPPKEY',
    await driver.findElement(By.css('.dimension-modal .ksd-mb-10:nth-child(4) .table-header')).click()
    await driver.sleep(1000)
    await driver.findElement(By.css('.dimension-modal .ksd-mb-10:nth-child(4) .el-table .guide-SUPPLIERS_SUPPKEY .el-checkbox')).click()
    await driver.sleep(500)
    await driver.findElement(By.css('.dimension-modal .ksd-mb-10:nth-child(4) .table-header')).click()
    await driver.sleep(1000)
    // 'PART.P_PARTKEY'
    await driver.findElement(By.css('.dimension-modal .ksd-mb-10:nth-child(5) .table-header')).click()
    await driver.sleep(1000)
    await driver.findElement(By.css('.dimension-modal .ksd-mb-10:nth-child(5) .el-table .guide-PARTP_PARTKEY .el-checkbox')).click()
    await driver.sleep(500)
    await driver.findElement(By.css('.dimension-modal .ksd-mb-10:nth-child(5) .table-header')).click()
    await driver.sleep(1000)
    await driver.findElement(By.css('.dimension-modal .el-dialog__footer .el-button:nth-child(2)')).click()
    await driver.wait(until.elementIsNotVisible(driver.findElement(By.css('.dimension-modal'))))

    // 添加度量
    // [{
    //   expression: 'SUM(column)',
    //   parameter: 'P_LINEORDER.LO_REVENUE'
    // }, {
    //   expression: 'SUM(column)',
    //   parameter: 'P_LINEORDER.LO_SUPPLYCOST'
    // }, {
    //   expression: 'SUM(column)',
    //   parameter: 'P_LINEORDER.V_REVENUE'
    // }, {
    //   expression: 'COUNT_DISTINCT',
    //   parameter: 'P_LINEORDER.LO_LINENUMBER'
    // }]
    await driver.findElement(By.css('.tool-icon-group .tool-icon:nth-child(1)')).click()
    await driver.sleep(1000)
    await addMeasure(driver, 'testMeasure1', 'SUM(column)', 'LO_REVENUE')
    await addMeasure(driver, 'testMeasure2', 'SUM(column)', 'LO_SUPPLYCOST')
    await addMeasure(driver, 'testMeasure3', 'SUM(column)', 'V_REVENUE')
    await addMeasure(driver, 'testMeasure4', 'COUNT_DISTINCT', 'LO_LINENUMBER')

    // 保存模型
    await driver.findElement(By.css('.mode-edit-tabs .footer .el-button:nth-child(2)')).click()
    await driver.wait(until.elementIsVisible(driver.findElement(By.css('.model-partition-dialog'))))
    await changeFormSelect(driver, '.model-partition-dialog .partition-column-format', '.js_partition-column-format .el-select-dropdown__list', 6)
    await changeFormSelect(driver, '.model-partition-dialog .partition-column', '.js_partition-column .el-select-dropdown__list', 6)
    await changeFormSelect(driver, '.model-partition-dialog .partition-column-format', '.js_partition-column-format .el-select-dropdown__list', 1)
    await driver.findElement(By.css('.model-partition-dialog .el-dialog__footer .el-button:nth-child(2)')).click()
    await driver.wait(until.elementIsNotVisible(driver.findElement(By.css('.model-partition-dialog'))))

    // 跳转去模型list页面，并展开至添加聚合组tab
    await driver.wait(until.elementIsVisible(driver.findElement(By.css('.add-index-confirm-dialog'))))
    await driver.findElement(By.css('.add-index-confirm-dialog .el-dialog__footer .el-button:nth-child(2)')).click()
    await driver.sleep(3000)
  })

  // 建索引
  it('在项目 PROJECT_NAME 下的模型 MODEL_NAME 建索引', async () => {
    // 如果有引导遮罩，点击忽略
    let mask = null
    try {
      mask = await driver.findElement(By.css('.model-guide-mask'))
    } catch (e) {}
    if (mask) {
      await driver.findElement(By.css('.model-guide-mask .index-block .btn-group .el-button:nth-child(1)')).click()
      await driver.sleep(1000)
    }
    // // 临时处理：展开第一个模型
    // await driver.findElement(By.css('.model_list_table .el-icon-caret-right')).click()
    // await driver.sleep(1000)
    // await driver.findElement(By.id('tab-third')).click()
    // await driver.sleep(2000)

    // 添加聚合组
    await driver.findElement(By.css('.model-aggregate-view .el-icon-ksd-project_add')).click()
    await driver.sleep(1000)
    await driver.wait(until.elementIsVisible(driver.findElement(By.css('.aggregate-modal'))))
    // 包含维度
    await driver.findElement(By.css('.aggregate-modal .add-includes-btn')).click()
    await driver.wait(until.elementIsVisible(driver.findElement(By.css('.edit-includes-dimensions'))))
    await driver.findElement(By.css('.edit-includes-dimensions .table-header .el-checkbox')).click()
    await driver.sleep(1000)
    await driver.findElement(By.css('.edit-includes-dimensions .el-dialog__footer .el-button:nth-child(2)')).click()
    await driver.sleep(1000)
    // 必需维度
    await changeFormSelect(driver, '.aggregate-modal .mandatory-select', '.js_mandatory-select', 4)
    await driver.findElement(By.css('.aggregate-modal')).click()
    await driver.sleep(1000)
    // 层级维度
    await changeFormSelect(driver, '.aggregate-modal .hierarchy-select', '.js_hierarchy-select', [3, 7, 6])
    await driver.findElement(By.css('.aggregate-modal')).click()
    await driver.sleep(1000)
    // 联合维度
    await changeFormSelect(driver, '.aggregate-modal .joint-select', '.js_joint-select', [3, 2])
    await driver.findElement(By.css('.aggregate-modal')).click()
    await driver.sleep(1000)
    // 保存聚合组
    await driver.findElement(By.css('.aggregate-modal .dialog-footer .right .el-button:nth-child(3)')).click()
    await driver.sleep(3000)
    // 构建索引
    await driver.wait(until.elementIsVisible(driver.findElement(By.css('.model-build'))))
    await driver.findElement(By.css('.model-build .el-icon-ksd-data_range_search')).click()
    // await driver.wait(until.elementIsNotVisible(driver.findElement(By.css('.dialog-footer .el-button.is-disabled'))))
    await driver.sleep(2000)
    await driver.findElement(By.css('.model-build .dialog-footer .el-button:nth-child(2)')).click()
    await driver.sleep(4000)
    // 跳转到job页面
    await driver.wait(until.elementLocated(By.id('monitor')), 10000)
    await driver.findElement(By.id('monitor')).click()
    await driver.findElement(By.id('monitor')).findElement(By.css('.el-menu .el-menu-item:nth-child(1)')).click()
    await driver.sleep(2000)
  })

  // 查询
  it('查询一条 sql', async () => {
    // 点击跳转到模型页面
    await driver.wait(until.elementLocated(By.id('query')), 10000)
    await driver.findElement(By.id('query')).click()
    await driver.sleep(1000)
    await driver.findElement(By.id('query')).findElement(By.css('.el-menu .el-menu-item:nth-child(1)')).click()
    await driver.sleep(2000)
    await changeFormTextarea(driver, '.query_panel_box', 'select sum(lo_revenue) as revenue from ssb.lineorder left join ssb.dates on lo_orderdate = d_datekey where d_year = 1993 and lo_discount between 1 and 3 and lo_quantity < 25')
    await driver.sleep(1000)
    await driver.findElement(By.css('.query_panel_box .operator .el-form-item:last-child .el-button')).click()
    await driver.sleep(3000)
  })

  it('清除模型项目退出登录', async () => {
    // 点击跳转到模型页面
    await driver.wait(until.elementLocated(By.id('studio')), 10000)
    await driver.findElement(By.id('studio')).findElement(By.css('.el-menu .el-menu-item:nth-child(2)')).click()
    await driver.sleep(2000)
    // 删除模型
    await driver.findElement(By.css('.model_list_table .el-icon-ksd-table_others')).click()
    await driver.sleep(1000)
    await driver.findElement(By.css('.specialDropdown .el-dropdown-menu__item:nth-child(7')).click()
    await driver.wait(until.elementIsVisible(driver.findElement(By.css('.el-message-box'))))
    await driver.findElement(By.css('.el-message-box .el-message-box__btns .el-button:nth-child(2)')).click()
    await driver.sleep(1000)
    // 跳转系统管理页面
    await driver.findElement(By.css('.entry-admin')).click()
    await driver.sleep(4000)
    // 搜索项目删除项目
    await changeFormInput(driver, '.show-search-btn', PROJECT_NAME)
    await driver.sleep(1000)
    const actions = driver.actions({bridge: true})
    await actions.click(await driver.findElement(By.css('.show-search-btn'))).sendKeys(Key.ENTER).perform()
    await driver.sleep(1000)
    await driver.findElement(By.css('.project-table .el-icon-ksd-table_others')).click()
    await driver.sleep(1000)
    await driver.findElement(By.css('.project-dropdown .el-dropdown-menu__item:nth-child(4)')).click()
    await driver.sleep(1000)
    await driver.wait(until.elementIsVisible(driver.findElement(By.css('.el-message-box'))))
    await driver.findElement(By.css('.el-message-box .el-message-box__btns .el-button:nth-child(2)')).click()
    await driver.sleep(3000)
    // 登出
    await logout(driver)
  })
})
