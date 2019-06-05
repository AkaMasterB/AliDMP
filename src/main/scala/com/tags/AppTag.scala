package com.tags

import com.common.Tags
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

/**
  * 功能描述:
  * 〈 App标签 〉
  *
  * @since: 1.0.0
  * @Author:SiXiang
  * @Date: 2019/6/5 19:09
  */
object AppTag extends Tags {
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    val row = args(0).asInstanceOf[Row]
    val dicts = args(1).asInstanceOf[Broadcast[Map[String, String]]]

    val appId = row.getAs[String]("appid")
    val appName = row.getAs[String]("appname")

    if (StringUtils.isNotBlank(appName)) {
      list :+= ("APP" + appName, 1)
    } else if (StringUtils.isNotBlank(appId)) {
      list :+= ("APP" + dicts.value.getOrElse(appId, appId), 1)
    }
    list
  }
}
