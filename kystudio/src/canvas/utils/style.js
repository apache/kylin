export function withoutPaddings (sizeType, boxSize, paddings) {
  let paddingSize = 0

  if (sizeType === 'height') {
    paddingSize = paddings.top + paddings.bottom
  } else if (sizeType === 'width') {
    paddingSize = paddings.left + paddings.right
  }

  return boxSize > paddingSize ? boxSize - paddingSize : 0
}

export function getPaddings (padding) {
  const paddings = padding.toString().split(/\s+/g).map(p => +p)
  switch (paddings.length) {
    case 1: return { top: paddings[0], right: paddings[0], bottom: paddings[0], left: paddings[0] }
    case 2: return { top: paddings[0], right: paddings[1], bottom: paddings[0], left: paddings[1] }
    case 3: return { top: paddings[0], right: paddings[1], bottom: paddings[2], left: paddings[1] }
    case 4: return { top: paddings[0], right: paddings[1], bottom: paddings[2], left: paddings[3] }
    default: return { top: 0, right: 0, bottom: 0, left: 0 }
  }
}

export function getContentSize (fixSize, minSize, maxSize, sampleSize, paddings) {
  const contentSize = { width: 0, height: 0 }

  for (const type of ['width', 'height']) {
    if (fixSize[type]) {
      contentSize[type] = withoutPaddings(type, fixSize[type], paddings)
    } else if (minSize[type] > sampleSize[type]) {
      contentSize[type] = withoutPaddings(type, minSize[type], paddings)
    } else if (maxSize[type] < sampleSize[type]) {
      contentSize[type] = withoutPaddings(type, maxSize[type], paddings)
    } else {
      contentSize[type] = withoutPaddings(type, sampleSize[type], paddings)
    }
  }
  return contentSize
}

export function getBoxSize (contentSize, paddings) {
  return {
    width: contentSize.width + paddings.left + paddings.right,
    height: contentSize.height + paddings.top + paddings.bottom
  }
}
