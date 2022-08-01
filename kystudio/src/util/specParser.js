import spec from '../config/spec'
import Vue from 'vue'

/**
 * 从spec中获取可用options
 * @param {*} type: 获取哪个类型的可用option
 * @param {*} parameters: 可用option的参数依据
 */
export function getAvailableOptions (type, parameters) {
  const { allOptionMaps, enableOptionMaps, disableOptionMaps } = spec
  const allOptionMap = allOptionMaps[type]
  const enableOptionMap = enableOptionMaps[type]
  const disableOptionMap = disableOptionMaps[type]

  let availableOptions = []

  if (allOptionMap) {
    availableOptions = allOptionMap.map(optionMap => optionMap.id)

    if (enableOptionMap) {
      const enableOptions = getMatchedOptions(enableOptionMap, type, parameters, allOptionMaps)
      availableOptions = mergeEnableOptions(availableOptions, enableOptions)
    }

    if (disableOptionMap) {
      const disableOptions = getMatchedOptions(disableOptionMap, type, parameters, allOptionMaps)
      availableOptions = mergeDisableOptions(availableOptions, disableOptions)
    }
  }

  return allOptionMap
    .filter(optionMap => availableOptions.includes(optionMap.id))
    .map(optionMap => optionMap.value || optionMap.id)
}

/**
 * 获取匹配的options
 * @param {*} param0
 * @param {*} type
 * @param {*} parameters
 * @param {*} allOptionMaps
 */
function getMatchedOptions ({ keyPattern = '', entries = [] }, type = '', parameters = {}, allOptionMaps = []) {
  for (const entry of entries) {
    if (isMatchEntryPattern(keyPattern, entry.key, parameters, allOptionMaps)) {
      return entry.value !== 'none' ? entry.value.split(',') : []
    }
  }
  return []
}

/**
 * 判断option map中的entry是否匹配pattern
 * @param {*} keyPattern
 * @param {*} entryKeyPattern
 * @param {*} parameters
 * @param {*} allOptionMaps
 */
function isMatchEntryPattern (keyPattern, entryKeyPattern, parameters, allOptionMaps) {
  const keyPatterns = keyPattern.split('-')
  const entryPatterns = entryKeyPattern.split('-')

  return keyPatterns.every((pattern, index) => {
    const entryPattern = entryPatterns[index]
    const parameterValues = parameters[pattern] ? (parameters[pattern] instanceof Array ? parameters[pattern] : [parameters[pattern]]) : []
    const allOptionMap = allOptionMaps[pattern] || []
    const currentOptionMap = allOptionMap.find(optionMap => parameterValues.includes(optionMap.value)) || {}
    const parameterKeys = currentOptionMap.id ? [currentOptionMap.id] : parameterValues

    checkVaild(pattern, parameters, allOptionMaps)

    if (isArray(entryPattern)) {
      const arrayPattern = string2Array(entryPattern)
      return parameterKeys.some(parameterKey => arrayPattern.includes(parameterKey))
    } else if (entryPattern === '*') {
      return true
    } else {
      return parameterKeys.some(parameterKey => entryPattern === parameterKey)
    }
  })
}

function mergeEnableOptions (availableOptions, enableOptions) {
  return availableOptions.filter(availableOption => enableOptions.includes(availableOption))
}

function mergeDisableOptions (availableOptions, disableOptions) {
  return availableOptions.filter(availableOption => !disableOptions.includes(availableOption))
}

function isArray (str) {
  return str.indexOf('[') === 0 && str.indexOf(']') === str.length - 1
}

function string2Array (str) {
  return str.replace(/^\[/g, '').replace(/\]$/g, '').split(',')
}

function checkVaild (pattern, parameters, allOptionMaps) {
  !parameters[pattern] && warn(`Key Pattern cannot get '${pattern}' in parameters, please check parameters`)
  !allOptionMaps[pattern] && warn(`Key Pattern cannot get '${pattern}' in allOptionMaps, please check allOptionMaps in spec.js`)
}

function warn (msg) {
  Vue.util.warn(msg)
}
