const { Builder, By, until, Key } = require('selenium-webdriver')
const assert = require('assert')
const { login, logout, delUser } = require('../utils/businessHelper')
const { changeFormInput, clearFormInput } = require('../utils/domHelper')

const {
  BROWSER_ENV,
  LAUNCH_URL,
  USERNAME_ADMIN,
  PASSWORD_ADMIN,
  PASSWORD_ADMIN_NEW,
  PROJECT_NAME,
  USERNAME_PROJECT_ADMIN,
  USERNAME_PROJECT_MANAGEMENT,
  USERNAME_PROJECT_OPERATION,
  USERNAME_PROJECT_QUERY
} = process.env

/* eslint-disable newline-per-chained-call */
describe('系统管理员进入系统', async function () {
  this.timeout(60000)
  let driver

  before(async () => {
    driver = await new Builder().forBrowser(BROWSER_ENV).build()
  })

  after(async () => {
    await driver.quit()
  })

  it('删除自动化测试创建的用户', async () => {
    await driver.get(LAUNCH_URL)
    await driver.manage().window().setRect(1440, 828)
    // 统一调用登录
    await login(driver, USERNAME_ADMIN, PASSWORD_ADMIN_NEW);
    await driver.sleep(2000)

    await driver.findElement(By.css('.entry-admin')).click()
    // await driver.sleep(1000)
    await driver.wait(until.elementLocated(By.css('.el-menu-item .el-icon-ksd-table_admin')), 10000)
    // 点击菜单 用户
    await driver.findElement(By.css('.el-menu-item .el-icon-ksd-table_admin')).click()
    await driver.sleep(2000)
  })

  it('删除项目 admin', async () => {
    await delUser(driver, USERNAME_PROJECT_ADMIN, 2)
    await driver.sleep(2000)
  })

  it('删除项目 management', async () => {
    await delUser(driver, USERNAME_PROJECT_MANAGEMENT, 2)
    await driver.sleep(2000)
  })

  it('删除项目 operation', async () => {
    await delUser(driver, USERNAME_PROJECT_OPERATION, 2)
    await driver.sleep(2000)
  })

  it('删除项目 query', async () => {
    await delUser(driver, USERNAME_PROJECT_QUERY, 2)
    await driver.sleep(2000)
  })

  /* it('删除模型', async () => {

  }) */

  it('删除项目', async () => {
    // 点击菜单 项目
    await driver.findElement(By.css('.el-menu-item .el-icon-ksd-project_list')).click()
    await driver.sleep(2000)

    await changeFormInput(driver, '.show-search-btn', PROJECT_NAME)
    await driver.sleep(1000)

    const actions = driver.actions({bridge: true})
    await actions.click(await driver.findElement(By.css('.show-search-btn input'))).sendKeys(Key.ENTER).perform()
    await driver.sleep(2000)
    await driver.findElement(By.css('.el-icon-ksd-table_others')).click()
    
    let moreBtnEl = await driver.findElement(By.css('.el-icon-ksd-table_others'))
    let dropdownMenuId = await moreBtnEl.getAttribute('aria-controls')
  
    await driver.actions().move({ origin: moreBtnEl }).perform()
    await driver.sleep(1000)

    await driver.wait(until.elementLocated(By.id(dropdownMenuId)), 10000)
    await driver.findElement(By.css(`ul#${dropdownMenuId} .el-dropdown-menu__item:nth-child(4)`)).click()

    await driver.wait(until.elementIsVisible(driver.findElement(By.css('div[aria-label=删除项目] .el-message-box__btns .el-button--primary'))), 10000)
    await driver.findElement(By.css('div[aria-label=删除项目] .el-message-box__btns .el-button--primary')).click()
    await driver.sleep(2000)
  })
})
