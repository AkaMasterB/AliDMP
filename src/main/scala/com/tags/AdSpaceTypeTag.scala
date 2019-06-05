package com.tags

import com.common.Tags
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * 功能描述:
  * 〈 广告位标签 〉
  *
  * @since: 1.0.0
  * @Author:SiXiang
  * @Date: 2019/6/5 19:08
  */
object AdSpaceTypeTag extends Tags {
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    val row = args(0).asInstanceOf[Row]

    val AdSpaceType = row.getAs[Int]("adspacetype")
    val AdSpaceTypeName = row.getAs[String]("adspacetypename")

    if (AdSpaceType != null) {
      AdSpaceType match {
        case v if v > 9 => list :+= ("LC" + v, 1)
        case v if v <= 9 => list :+= ("LC0" + v, 1)
      }
    }
    if (StringUtils.isNotBlank(AdSpaceTypeName)) {
      list :+= ("LN" + AdSpaceTypeName, 1)
    }
    list
  }
}
