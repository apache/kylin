package org.apache.kylin.engine.spark.metadata

import org.apache.kylin.cube.{CubeInstance, CubeUpdate}

object MetadataConverter {
  def getSegmentInfo(cubeInstance: CubeInstance): SegmentInfo = {
    val model = cubeInstance.getModel
    val dimensionMap = cubeInstance.getDescriptor
      .getRowkey
      .getRowKeyColumns
      .map(co => (co.getColRef, co.getBitIndex))
      .toMap


    null
  }
  def  getCubeUpdate(segmentInfo:SegmentInfo): CubeUpdate = {
    null
  }
}
