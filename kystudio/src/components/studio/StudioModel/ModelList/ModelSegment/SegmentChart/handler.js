const MILLISECOND = 'millisecond'
const SECOND = 'second'
const MINUTE = 'minute'
const HOUR = 'hour'
const DAY = 'day'
const MONTH = 'month'
const YEAR = 'year'

export const scaleTypes = {
  MILLISECOND,
  SECOND,
  MINUTE,
  HOUR,
  DAY,
  MONTH,
  YEAR
}

export const formatTypes = {
  [HOUR]: 'HH:mm A',
  [DAY]: 'MMM DD',
  [MONTH]: 'MMM',
  [YEAR]: 'YYYY'
}

export function isFilteredSegmentsContinue (segment, selectedSegments) {
  const isSegmentSelected = selectedSegments.some(selectedSegment => selectedSegment.id === segment.id)
  const filteredSegments = isSegmentSelected ? selectedSegments.filter(selectSegment => segment.id !== selectSegment.id) : [...selectedSegments, segment]

  filteredSegments.sort((segmentA, segmentB) => segmentA.startTime < segmentB.startTime ? -1 : 1)

  return filteredSegments.every((currentSegment, index) => {
    const lastSegment = filteredSegments[index - 1]

    return lastSegment ? lastSegment.endTime === currentSegment.startTime : true
  })
}
