from time import sleep

from getgauge.python import step
from kylin_utils import util


class LoginTest:

    @step("Initialize <browser_type> browser and connect to <file_name>")
    def setup_browser(self, browser_type, file_name):
        global browser
        browser = util.setup_browser(browser_type=browser_type)

        browser.get(util.kylin_url(file_name))
        sleep(3)

        browser.refresh()
        browser.set_window_size(1400, 800)

    @step("Authentication with user <user> and password <password>.")
    def assert_authentication_failed(self, user, password):
        browser.find_element_by_id("username").clear()
        browser.find_element_by_id("username").send_keys(user)
        browser.find_element_by_id("password").clear()
        browser.find_element_by_id("password").send_keys(password)

        browser.find_element_by_class_name("bigger-110").click()

    @step("Authentication with built-in user <table>")
    def assert_authentication_success(self, table):
        for i in range(1, 2):
            user = table.get_row(i)
            browser.find_element_by_id("username").clear()
            browser.find_element_by_id("username").send_keys(user[0])
            browser.find_element_by_id("password").clear()
            browser.find_element_by_id("password").send_keys(user[1])
            browser.find_element_by_class_name("bigger-110").click()
