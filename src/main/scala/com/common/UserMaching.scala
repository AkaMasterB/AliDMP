package com.common

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object UserMaching {
  def getAnyUserId(row: Row): String = {
    row match {
      case v if StringUtils.isNoneBlank(v.getAs[String]("imei")) => "IM:" + v.getAs[String]("imei")
      case v if StringUtils.isNoneBlank(v.getAs[String]("mac")) => "MC:" + v.getAs[String]("mac")
      case v if StringUtils.isNoneBlank(v.getAs[String]("idfa")) => "ID:" + v.getAs[String]("idfa")
      case v if StringUtils.isNoneBlank(v.getAs[String]("openudid")) => "OD:" + v.getAs[String]("openudid")
      case v if StringUtils.isNoneBlank(v.getAs[String]("androidid")) => "AOD:" + v.getAs[String]("androidid")
      case v if StringUtils.isNoneBlank(v.getAs[String]("imeimd5")) => "IMM:" + v.getAs[String]("imeimd5")
      case v if StringUtils.isNoneBlank(v.getAs[String]("macmd5")) => "MCM:" + v.getAs[String]("macmd5")
      case v if StringUtils.isNoneBlank(v.getAs[String]("idfamd5")) => "IDM:" + v.getAs[String]("idfamd5")
      case v if StringUtils.isNoneBlank(v.getAs[String]("openudidmd5")) => "ODM:" + v.getAs[String]("openudidmd5")
      case v if StringUtils.isNoneBlank(v.getAs[String]("androididmd5")) => "AODM:" + v.getAs[String]("androididmd5")
      case v if StringUtils.isNoneBlank(v.getAs[String]("imeisha1")) => "IMS:" + v.getAs[String]("imeisha1")
      case v if StringUtils.isNoneBlank(v.getAs[String]("macsha1")) => "MCS:" + v.getAs[String]("macsha1")
      case v if StringUtils.isNoneBlank(v.getAs[String]("idfasha1")) => "IDS:" + v.getAs[String]("idfasha1")
      case v if StringUtils.isNoneBlank(v.getAs[String]("openudidsha1")) => "ODS:" + v.getAs[String]("openudidsha1")
      case v if StringUtils.isNoneBlank(v.getAs[String]("androididsha1")) => "AODS:" + v.getAs[String]("androididsha1")
    }
  }

  val hasNeedOneUserId =
    """
      |imei!='' or mac != '' or idfa != '' or openudid !='' or androidid !='' or
      |imeimd5!='' or macmd5 != '' or idfamd5 != '' or openudidmd5 !='' or androididmd5 !='' or
      |imeisha1!='' or macsha1 != '' or idfasha1 != '' or openudidsha1 !='' or androididsha1 !=''
    """.stripMargin
}
