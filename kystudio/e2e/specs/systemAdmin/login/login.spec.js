const { Builder, By, until } = require('selenium-webdriver')
const assert = require('assert')
// const { clearFormInput } = require('../../utils/domHelper')
const { closeLicenseBox, waitingForPageClean } = require('../../utils/businessHelper')

const {
  BROWSER_ENV,
  LAUNCH_URL,
  USERNAME_ADMIN,
  PASSWORD_ADMIN
} = process.env

/* eslint-disable newline-per-chained-call */
describe('系统管理员登录', async function () {
  this.timeout(30000)
  let driver

  before(async () => {
    driver = await new Builder().forBrowser(BROWSER_ENV).build()
  })

  after(async () => {
    await driver.quit()
  })

  // 异常用例
  it('空的表单登录', async () => {
    await driver.get(LAUNCH_URL)
    await driver.manage().window().setRect(1840, 828)

    await driver.findElement(By.css('.login-form .el-button--primary')).click()
    await driver.sleep(1000)

    const usernameString = await driver.findElement(By.css('.login-form .input_group .el-form-item:nth-child(1) .el-form-item__error')).getText()
    assert.equal(usernameString, '请输入用户名')

    const passwordString = await driver.findElement(By.css('.login-form .input_group .el-form-item:nth-child(2) .el-form-item__error')).getText()
    assert.equal(passwordString, '请输入密码')
  })

  /* it('错误的用户名', async () => {
    await waitingForPageClean(driver)
    await clearFormInput(driver, '.login-form .input_group .el-form-item:nth-child(1) input')
    await driver.findElement(By.css('.login-form .input_group .el-form-item:nth-child(1) input')).sendKeys('error_error_error')
    await clearFormInput(driver, '.login-form .input_group .el-form-item:nth-child(2) input')
    await driver.findElement(By.css('.login-form .input_group .el-form-item:nth-child(2) input')).sendKeys('error_error_error')
    await driver.findElement(By.css('.login-form .el-button--primary')).click()

    await driver.wait(until.elementIsVisible(driver.findElement(By.css('.errMsgBox'))), 10000)
    const errorString = await driver.findElement(By.css('.errMsgBox .error-title')).getText()
    assert.equal(errorString.includes('找不到用户'), true)

    await driver.executeScript(`
      var button = document.querySelector(".errMsgBox .dialog-footer .el-button--default")
      button.dispatchEvent(new Event("click"))
    `)
  })

  it('错误的密码一次', async () => {
    await waitingForPageClean(driver)
    await clearFormInput(driver, '.login-form .input_group .el-form-item:nth-child(1) input')
    await driver.findElement(By.css('.login-form .input_group .el-form-item:nth-child(1) input')).sendKeys(USERNAME_ADMIN)
    await clearFormInput(driver, '.login-form .input_group .el-form-item:nth-child(2) input')
    await driver.findElement(By.css('.login-form .input_group .el-form-item:nth-child(2) input')).sendKeys('error_error_error')
    await driver.findElement(By.css('.login-form .el-button--primary')).click()

    await driver.wait(until.elementIsVisible(driver.findElement(By.css('.errMsgBox .error-title'))), 10000)
    const errorString = await driver.findElement(By.css('.errMsgBox .error-title')).getText()
    assert.equal(errorString.includes('用户名或密码错误。'), true)

    await driver.executeScript(`
      var button = document.querySelector(".errMsgBox .dialog-footer .el-button--default")
      button.dispatchEvent(new Event("click"))
    `)
  })

  it('错误的密码二次', async () => {
    await waitingForPageClean(driver)
    await clearFormInput(driver, '.login-form .input_group .el-form-item:nth-child(1) input')
    await driver.findElement(By.css('.login-form .input_group .el-form-item:nth-child(1) input')).sendKeys(USERNAME_ADMIN)
    await clearFormInput(driver, '.login-form .input_group .el-form-item:nth-child(2) input')
    await driver.findElement(By.css('.login-form .input_group .el-form-item:nth-child(2) input')).sendKeys('error_error_error')
    await driver.findElement(By.css('.login-form .el-button--primary')).click()

    await driver.wait(until.elementIsVisible(driver.findElement(By.css('.errMsgBox .error-title'))), 10000)
    const errorString = await driver.findElement(By.css('.errMsgBox .error-title')).getText()
    assert.equal(errorString.includes('用户名或密码错误。'), true)

    await driver.executeScript(`
      var button = document.querySelector(".errMsgBox .dialog-footer .el-button--default")
      button.dispatchEvent(new Event("click"))
    `)
  })

  it('错误的密码三次', async () => {
    await waitingForPageClean(driver)
    await clearFormInput(driver, '.login-form .input_group .el-form-item:nth-child(1) input')
    await driver.findElement(By.css('.login-form .input_group .el-form-item:nth-child(1) input')).sendKeys(USERNAME_ADMIN)
    await clearFormInput(driver, '.login-form .input_group .el-form-item:nth-child(2) input')
    await driver.findElement(By.css('.login-form .input_group .el-form-item:nth-child(2) input')).sendKeys('error_error_error')
    await driver.findElement(By.css('.login-form .el-button--primary')).click()

    await driver.wait(until.elementIsVisible(driver.findElement(By.css('.errMsgBox .error-title'))), 10000)
    const errorString = await driver.findElement(By.css('.errMsgBox .error-title')).getText()
    assert.equal(errorString.includes('用户名或密码错误，请在30秒后再次重试'), true)

    await driver.executeScript(`
      var button = document.querySelector(".errMsgBox .dialog-footer .el-button--default")
      button.dispatchEvent(new Event("click"))
    `)
  }) */

  it('用户管理员登录', async () => {
    // 前面的多次尝试错误，会锁住 30 秒，保险起见，等待 31 秒后进行登录
    // await driver.sleep(31000)
    await waitingForPageClean(driver)
    await driver.findElement(By.css('.login-form .input_group .el-form-item:nth-child(1) input')).sendKeys(USERNAME_ADMIN)
    await driver.findElement(By.css('.login-form .input_group .el-form-item:nth-child(2) input')).sendKeys(PASSWORD_ADMIN)
    await driver.findElement(By.css('.login-form .el-button--primary')).click()

    await driver.wait(until.elementLocated(By.css('.topbar .limit-user-name')), 10000)
    assert.equal(await driver.findElement(By.css('.topbar .limit-user-name')).getText(), USERNAME_ADMIN)

    await closeLicenseBox(driver)
  })
})
