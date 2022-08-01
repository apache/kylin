const { Builder, By, until } = require('selenium-webdriver')
const assert = require('assert')
const { login, logout } = require('../../utils/businessHelper')
const { changeFormInput, changeFormTextarea } = require('../../utils/domHelper')

const {
  BROWSER_ENV,
  LAUNCH_URL,
  PROJECT_NAME,
  USERNAME_PROJECT_OPERATION,
  PASSWORD_PROJECT_OPERATION
} = process.env

/* eslint-disable newline-per-chained-call */
describe('项目 operation', async function () {
  this.timeout(30000)
  let driver

  before(async () => {
    driver = await new Builder().forBrowser(BROWSER_ENV).build()
  })

  after(async () => {
    await driver.quit()
  })

  it('项目 operation 登录', async () => {
    await driver.get(LAUNCH_URL)
    await driver.manage().window().setRect(1440, 828)

    // 统一调用登录
    await login(driver, USERNAME_PROJECT_OPERATION, PASSWORD_PROJECT_OPERATION);
    await driver.sleep(2000)

    assert.equal(await driver.findElement(By.css('.topbar .limit-user-name')).getText(), USERNAME_PROJECT_OPERATION)

    await closeLicenseBox(driver)
  })
})
