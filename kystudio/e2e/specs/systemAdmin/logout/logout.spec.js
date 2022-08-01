const { Builder, By, until } = require('selenium-webdriver')
const assert = require('assert')
const { clearFormInput } = require('../../utils/domHelper')
const { closeLicenseBox, waitingForPageClean } = require('../../utils/businessHelper')

const {
  BROWSER_ENV,
  LAUNCH_URL,
  USERNAME_ADMIN,
  PASSWORD_ADMIN
} = process.env

/* eslint-disable newline-per-chained-call */
describe('登出', async function () {
  this.timeout(30000)
  let driver

  before(async () => {
    driver = await new Builder().forBrowser(BROWSER_ENV).build()
  })

  after(async () => {
    await driver.quit()
  })

  it('用户管理员登出', async () => {
    // 登录的用例在其他用例之前都要执行了
    await driver.get(LAUNCH_URL)
    await driver.manage().window().setRect(1440, 828)
    await waitingForPageClean(driver)
    await driver.findElement(By.css('.login-form .input_group .el-form-item:nth-child(1) input')).sendKeys(USERNAME_ADMIN)
    await driver.findElement(By.css('.login-form .input_group .el-form-item:nth-child(2) input')).sendKeys(PASSWORD_ADMIN)
    await driver.findElement(By.css('.login-form .el-button--primary')).click()

    // 从登录页进来到登出需要等待下
    await driver.sleep(3000)
    const usernameEl = await driver.findElement(By.css('.topbar .user-msg-dropdown .el-dropdown-link'))
    const dropdownMenuId = await usernameEl.getAttribute('aria-controls')
    await driver.actions().move({ origin: usernameEl }).perform()
    await driver.sleep(1000)

    await driver.wait(until.elementLocated(By.id(dropdownMenuId)), 10000)
    await driver.findElement(By.css(`ul#${dropdownMenuId} .el-dropdown-menu__item:nth-child(3)`)).click()

    await driver.wait(until.elementLocated(By.css('.el-message-box')), 10000)
    await driver.findElement(By.css(`.el-message-box .el-button--primary`)).click()

    await driver.wait(until.elementLocated(By.css('.login-form')), 10000)
  })
})
