export function formatSegments (that, segments) {
  return segments.map(segment => {
    const isFullLoad = segment.segRange.date_range_start === 0 && segment.segRange.date_range_end === 9223372036854776000
    return {
      ...segment,
      segmentPath: segment.additionalInfo && segment.additionalInfo.segment_path,
      fileNumber: segment.additionalInfo && segment.additionalInfo.file_count,
      startTime: isFullLoad ? that.$t('fullLoad') : segment.segRange.date_range_start,
      endTime: isFullLoad ? that.$t('fullLoad') : segment.segRange.date_range_end
    }
  })
}
export function formatStreamSegments (that, segments) {
  return segments.map(segment => {
    const isFullLoad = segment.segRange.source_offset_start === 0 && segment.segRange.source_offset_end === 9223372036854776000
    return {
      ...segment,
      segmentPath: segment.additionalInfo && segment.additionalInfo.segment_path,
      fileNumber: segment.additionalInfo && segment.additionalInfo.file_count,
      startTime: isFullLoad ? that.$t('fullLoad') : segment.segRange.source_offset_start,
      endTime: isFullLoad ? that.$t('fullLoad') : segment.segRange.source_offset_end
    }
  })
}
