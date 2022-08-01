function getPoints (position, node) {
  switch (position) {
    case 'top': return { x: node.x + node.width / 2, y: node.y }
    case 'right': return { x: node.x + node.width, y: node.y + node.height / 2 }
    case 'bottom': return { x: node.x + node.width / 2, y: node.y + node.height }
    case 'left': return { x: node.x, y: node.y + node.height / 2 }
    default: return { x: node.x, y: node.y }
  }
}

function getDistanceBetween (sourcePoint, targetPoint) {
  return Math.sqrt(
    Math.pow((targetPoint.y - sourcePoint.y), 2) +
    Math.pow((targetPoint.x - sourcePoint.x), 2)
  )
}

function getShortestPointPair (sourcePoints = [], targetPoints = []) {
  let minDistance = Infinity
  let source = null
  let target = null

  for (const sourcePoint of sourcePoints) {
    for (const targetPoint of targetPoints) {
      const distance = getDistanceBetween(sourcePoint, targetPoint)
      if (minDistance > distance) {
        minDistance = distance
        source = sourcePoint
        target = targetPoint
      }
    }
  }

  return { source, target }
}

export function getConnectorPoints (sourceSize, targetSize) {
  const sourcePoints = ['bottom'].map(position => getPoints(position, sourceSize))
  const targetPoints = ['top'].map(position => getPoints(position, targetSize))

  return getShortestPointPair(sourcePoints, targetPoints)
}

export function getCenterPoint (source, target) {
  const x = source.x + (target.x - source.x) / 2
  const y = source.y + (target.y - source.y) / 2
  return { x, y }
}
