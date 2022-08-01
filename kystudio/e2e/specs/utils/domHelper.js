const { By, until } = require('selenium-webdriver');

/**
 * Hover在某个元素上
 * @param {WebDriver} driver WebDriver对象
 * @param {Locator} locator Hover元素的选择器
 */
async function hoverOn(driver, locator) {
  try {
    await driver.wait(until.elementLocated(locator), 1000);
  } catch (e) {}

  const el = await driver.findElement(locator);
  await driver.actions().move({ origin: el }).perform();
}

/**
 * 等待页面上所有的对话框和消息框消失
 * @param {WebDriver} driver WebDriver对象
 */
async function waitingForPageClean(driver) {
  try {
    const messageBoxWrappers = await driver.findElements(By.css('.el-message-box__wrapper'));
    for (const messageBoxWrapper of messageBoxWrappers) {
      try {
        await driver.wait(until.elementIsNotVisible(messageBoxWrapper), 1000);
      } catch (e) {}
    }
  } catch (e) {}

  try {
    const dialogWrappers = await driver.findElements(By.css('.el-dialog__wrapper'));
    for (const dialogWrapper of dialogWrappers) {
      try {
        await driver.wait(until.elementIsNotVisible(dialogWrapper), 1000);
      } catch (e) {}
    }
  } catch (e) {}
}

/**
 * 等待页面稳定500ms
 * @param {WebDriver} driver WebDriver对象
 */
async function waitingForStable(driver) {
  await driver.sleep(500);
}

/**
 * Author: jie.luo
 * 等待容器的loading消失
 * @param {WebDriver} driver WebDriver对象
 * @param {String} loadingParentClass loading容器类字符串
 */
async function waitingForLoading(driver, loadingParentClass) {
  try {
    await driver.wait(until.elementLocated(By.css(`${loadingParentClass} > div > div > div.el-loading-spinner`)), 1000);
  } catch (e) {}
  try {
    await driver.wait(until.stalenessOf(await driver.findElement(By.css(`${loadingParentClass} > div > div > div.el-loading-spinner`))), 1000);
  } catch (e) {}
}

/**
 * Author: jie.luo
 * JS辅助：清除表单输入框的值
 * @param {WebDriver} driver WebDriver对象
 * @param {String} selector 输入框的选择器字符串
 */
async function clearFormInput(driver, selector) {
  await driver.executeScript(`
    var input = document.querySelector("${selector}");
    input.value = "";
    // dispatchEvent触发的是原生的event，不是react event。此处有待出解决方案。
    // input.dispatchEvent(new Event("change"));
  `);
}

/**
 * JS辅助：在浏览器端打印内容
 * @param {WebDriver} driver WebDriver对象
 * @param  {...any} args 打印内容
 */
async function logInBrowser(driver, ...args) {
  const messages = args.map(arg => {
    switch (typeof arg) {
      case 'object': return `JSON.parse(JSON.stringify(${arg}))`;
      case 'string': return `'${arg}'`;
      default: return arg.toString();
    }
  });
  const scripts = messages.join(', ');
  await driver.executeScript(`
    console.log(${scripts});
  `);
}

/**
 * JS辅助：在浏览器端监听打印鼠标按下、移动、抬起事件
 * @param {WebDriver} driver WebDriver对象
 */
async function traceMouseEvents(driver) {
  await driver.executeScript(`
    try {
      if (!handleMouseDown) {
        var handleMouseDown = function handleMouseDown(e) {
          console.log('mousedown', e.clientX, e.clientY);
        }
      }
    } catch (e) {
      var handleMouseDown = function handleMouseDown(e) {
        console.log('mousedown', e.clientX, e.clientY);
      }
    }
    try {
      if (!handleMouseMove) {
        var handleMouseDown = function handleMouseDown(e) {
          console.log('mousedown', e.clientX, e.clientY);
        }
      }
    } catch (e) {
      var handleMouseMove = function handleMouseMove(e) {
        console.log('mousedown', e.clientX, e.clientY);
      }
    }
    try {
      if (!handleMouseUp) {
        var handleMouseDown = function handleMouseDown(e) {
          console.log('mousedown', e.clientX, e.clientY);
        }
      }
    } catch (e) {
      var handleMouseUp = function handleMouseUp(e) {
        console.log('mousedown', e.clientX, e.clientY);
      }
    }
    window.removeEventListener('mousedown', handleMouseDown);
    window.removeEventListener('mousemove', handleMouseMove);
    window.removeEventListener('mouseup', handleMouseUp);
    window.addEventListener('mousedown', handleMouseDown);
    window.addEventListener('mousemove', handleMouseMove);
    window.addEventListener('mouseup', handleMouseUp);
  `);
}

/**
 * JS辅助：在浏览器端停止监听鼠标按下、移动、抬起事件
 * @param {WebDriver} driver WebDriver对象
 */
async function stopMouseEvents(driver) {
  await driver.executeScript(`
    try {
      if (!handleMouseDown) {
        var handleMouseDown = function handleMouseDown(e) {
          console.log('mousedown', e.clientX, e.clientY);
        }
      }
    } catch (e) {
      var handleMouseDown = function handleMouseDown(e) {
        console.log('mousedown', e.clientX, e.clientY);
      }
    }
    try {
      if (!handleMouseMove) {
        var handleMouseDown = function handleMouseDown(e) {
          console.log('mousedown', e.clientX, e.clientY);
        }
      }
    } catch (e) {
      var handleMouseMove = function handleMouseMove(e) {
        console.log('mousedown', e.clientX, e.clientY);
      }
    }
    try {
      if (!handleMouseUp) {
        var handleMouseDown = function handleMouseDown(e) {
          console.log('mousedown', e.clientX, e.clientY);
        }
      }
    } catch (e) {
      var handleMouseUp = function handleMouseUp(e) {
        console.log('mousedown', e.clientX, e.clientY);
      }
    }
    window.removeEventListener('mousedown', handleMouseDown);
    window.removeEventListener('mousemove', handleMouseMove);
    window.removeEventListener('mouseup', handleMouseUp);
  `);
}

/**
 * JS辅助：拖拽目标元素放置在容器上
 * @param {WebDriver} driver WebDriver对象
 * @param {String} dragSelector 拖拽元素的选择器字符串
 * @param {String} dropSelector 放置拖拽元素容器的选择器字符串
 */
async function dragAndDrop(driver, dragSelector, dropSelector, clientX = 0, clientY = 0) {
  await driver.executeScript(`
    function customEvent(typeOfEvent, x, y) {
      var event = document.createEvent("CustomEvent");
      event.initCustomEvent(typeOfEvent, true, true, null);
      event.dataTransfer = {
          data: {},
          setData: function (key, value) {
              this.data[key] = value;
          },
          getData: function (key) {
              return this.data[key];
          }
      };
      event.clientX = x;
      event.clientY = y;
      return event;
    }
    function dispatchEvent(element, event, transferData) {
      if (transferData !== undefined) {
          event.dataTransfer = transferData;
      }
      if (element.dispatchEvent) {
          element.dispatchEvent(event);
      } else if (element.fireEvent) {
          element.fireEvent("on" + event.type, event);
      }
    }
    (function() {
      var dragEl = document.querySelector('${dragSelector}');
      var dropEl = document.querySelector('${dropSelector}');
      var dragStartEvent = customEvent('dragstart');
      dispatchEvent(dragEl, dragStartEvent);
      var dropEvent = customEvent('drop', ${clientX}, ${clientY});
      dispatchEvent(dropEl, dropEvent, dragStartEvent.dataTransfer);
      var dragEndEvent = customEvent('dragend');
      dispatchEvent(dragEl, dragEndEvent, dropEvent.dataTransfer);
    })()
  `);
}

async function changeFormSelect(driver, selector, popoverSelector, optionIdx, isFilter) {
  if (!isFilter) {
    try {
      await driver.wait(until.elementLocated(By.css(`${selector} .el-input`)), 10000);
    } catch (e) {}
    await driver.findElement(By.css(`${selector} .el-input`)).click();
    await driver.sleep(1000)
  }
  if (optionIdx instanceof Array) {
    for (const idx of optionIdx) {
      await driver.wait(until.elementIsVisible(await driver.findElement(By.css(`${popoverSelector} .el-select-dropdown__item:nth-child(${idx})`))), 1000);
      await driver.findElement(By.css(`${popoverSelector} .el-select-dropdown__item:nth-child(${idx})`)).click();
    }
    try {
      await driver.findElement(By.css(`${popoverSelector} .el-kylin-more`)).click();
    } catch (e) {}
  } else {
    await driver.wait(until.elementIsVisible(await driver.findElement(By.css(`${popoverSelector} .el-select-dropdown__item:nth-child(${optionIdx})`))), 2000);
    await driver.findElement(By.css(`${popoverSelector} .el-select-dropdown__item:nth-child(${optionIdx})`)).click();
  }
}

async function changeFormInput(driver, selector, value) {
  try {
    await driver.wait(until.elementLocated(By.css(`${selector} input`)), 1000);
  } catch (e) {}
  await clearFormInput(driver, `${selector} input`);
  await driver.findElement(By.css(`${selector} input`)).sendKeys(value);
}

async function changeFormTextarea(driver, selector, value) {
  try {
    await driver.wait(until.elementLocated(By.css(`${selector} textarea`)), 1000);
  } catch (e) {}
  await clearFormInput(driver, `${selector} textarea`);
  await driver.findElement(By.css(`${selector} textarea`)).sendKeys(value);
}

async function changeFormCascader(driver, selector, optionsIdx) {
  try {
    await driver.wait(until.elementLocated(By.css(`${selector} input`)), 1000);
  } catch (e) {}
  await driver.findElement(By.css(`${selector} input`)).click();

  for (let i = 0; i < optionsIdx.length; i += 1) {
    const optionIdx = optionsIdx[i];
    try {
      await driver.sleep(1000);
      await driver.wait(until.elementLocated(By.css(`${selector} .el-cascader-menu:nth-child(${i + 1}) .el-cascader-menu__item:nth-child(${optionIdx})`)), 1000);
    } catch (e) {}
    await driver.findElement(By.css(`${selector} .el-cascader-menu:nth-child(${i + 1}) .el-cascader-menu__item:nth-child(${optionIdx})`)).click();
  }
}

exports.waitingForLoading = waitingForLoading;
exports.waitingForStable = waitingForStable;
exports.waitingForPageClean = waitingForPageClean;

exports.hoverOn = hoverOn;
exports.dragAndDrop = dragAndDrop;
exports.clearFormInput = clearFormInput;
exports.changeFormInput = changeFormInput;
exports.changeFormTextarea = changeFormTextarea;
exports.changeFormSelect = changeFormSelect;
exports.changeFormCascader = changeFormCascader;

exports.logInBrowser = logInBrowser;
exports.traceMouseEvents = traceMouseEvents;
exports.stopMouseEvents = stopMouseEvents;