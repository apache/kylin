import $ from 'jquery'

export function get (object, pathStr = '') {
  try {
    const newObject = $.extend(true, {}, object)
    const pathArray = pathStr.split('.')

    let curObject = newObject

    pathArray.forEach((path, index) => {
      curObject = curObject[path]
    })

    return curObject
  } catch (e) {
    // console.log(e)
    return null
  }
}

export function set (object, pathStr = '', value) {
  try {
    const newObject = $.extend(true, {}, object)
    const pathArray = pathStr.split('.')

    let curObject = newObject

    pathArray.forEach((path, index) => {
      if (index === pathArray.length - 1) {
        curObject[path] = value
      } else {
        if (!curObject[path]) {
          curObject[path] = {}
        }
        curObject = curObject[path]
      }
    })

    return newObject
  } catch (e) {
    // console.log(e)
    return null
  }
}

export function push (object, pathStr = '', value) {
  try {
    const newObject = $.extend(true, {}, object)
    const pathArray = pathStr.split('.')

    let curObject = newObject

    pathArray.forEach((path, index) => {
      if (index === pathArray.length - 1) {
        curObject[path].push(value)
      } else {
        curObject = curObject[path]
      }
    })

    return newObject
  } catch (e) {
    // console.log(e)
    return null
  }
}
