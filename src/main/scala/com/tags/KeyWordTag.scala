package com.tags

import com.common.Tags
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

/**
  * 功能描述:
  * 〈 关键字标签 〉
  *
  * @since: 1.0.0
  * @Author:SiXiang
  * @Date: 2019/6/5 19:40
  */
object KeyWordTag extends Tags {
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    val row = args(0).asInstanceOf[Row]
    val stopWords = args(1).asInstanceOf[Broadcast[Map[String, Int]]]

    row.getAs[String]("keywords")
      .split("[|]")
      .map(t => {
        t.filter(t => {
          !stopWords.value.contains(t.toString)
        })
      })
      .filter(word => {
        word.length >= 3 && word.length <= 8
      })
      .foreach(word => list :+= ("K" + word, 1))

    list
  }
}
