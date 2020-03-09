package org.apache.spark.sql.utils

import org.apache.kylin.engine.spark.metadata.SegmentInfo
import org.apache.kylin.engine.spark.metadata.cube.model.LayoutEntity

object CuboidLayoutChooser {
  def selectLayoutForBuild(segment: SegmentInfo, entity: LayoutEntity): LayoutEntity = {
    val candidate = segment.layouts
      .filter(index => index.fullyDerive(entity))
      .filter(layout => !segment.toBuildLayouts.contains(layout))
    if (candidate.isEmpty) {
      null
    } else {
      candidate.minBy(_.getRows)
    }
  }
}
