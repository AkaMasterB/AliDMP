package com.tags

import com.common.UserMaching
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object ContextTags {
  def main(args: Array[String]): Unit = {
    // 首先判断目录是否为空
    if (args.length != 5) {
      // 退出程序
      println("目录输入不正确，退出程序！")
      sys.exit()
    }
    // 创建一个集合 存储输入输出目录
    val Array(inputPath, outputPath, dictPath, stopWordPath, date) = args

    // 创建执行入口
    val conf = new SparkConf().setAppName("parquet").setMaster("local[*]")
      // 采用scala的序列化方式
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    sQLContext.setConf("spark.sql.parquet.compression.codec", "snappy")

    // 读取字典文件
    val dictMap = sc.textFile(dictPath)
      .map(_.split("\t", -1))
      .filter(_.length >= 5)
      .map(arr => (arr(4), arr(1)))
      .collect()
      .toMap
    val globalDict = sc.broadcast(dictMap)

    // 停用词库
    val stopWordMap = sc.textFile(stopWordPath)
      .map((_,0))
      .collect()
      .toMap
    val globalStopWord = sc.broadcast(stopWordMap)

    // 初始化hbase、创建表
    val jobConf = initHtable(sc, "tags")

    sQLContext.read.parquet(inputPath)
      .filter(UserMaching.hasNeedOneUserId)
      .map(row => {
        val userId = UserMaching.getAnyUserId(row)
        val adSpaceTypeTag = AdSpaceTypeTag.makeTags(row)
        val appTag = AppTag.makeTags(row, globalDict)
        val adPlatformTag = AdPlatformProviderTag.makeTags(row)
        val deviceTag = DeviceTag.makeTags(row)
        val kwdTag = KeyWordTag.makeTags(row, globalStopWord)
        val locTag = LocationTag.makeTags(row)
        (userId, adSpaceTypeTag ++ appTag ++ adPlatformTag ++ deviceTag ++ kwdTag ++ locTag)
      })
      .reduceByKey((list1, list2) => (list1 ::: list2)
        .groupBy(_._1)
        //        .mapValues(_.size)
        .mapValues(_.foldLeft[Int](0)(_ + _._2))
        .toList
      )
      .map {
        case (userId, userTags) => {
          // rowkey
          val put = new Put(Bytes.toBytes(userId))
          // data
          val tags = userTags.map(t => t._1 + "," + t._2).mkString(";")
          // 列簇--列--数据
          put.addImmutable(Bytes.toBytes("tags"), Bytes.toBytes(s"$date"), Bytes.toBytes(tags))
          (new ImmutableBytesWritable(), put)
        }
      }
      .saveAsHadoopDataset(jobConf)
  }

  /**
    * 功能描述:
    * 〈 初始化hbase，创建表 〉
    *
    * @param sc
    * @param columnFamily
    * @return:void
    * @since: 1.0.0
    * @Author:SiXiang
    * @Date: 2019/6/5 20:56
    */
  def initHtable(sc: SparkContext, columnFamily: String) = {
    // 加载HBase配置信息
    val loader = ConfigFactory.load()
    val hbaseTbName = loader.getString("hbase_table_name")

    val hadoopConf = sc.hadoopConfiguration
    hadoopConf.set("hbase.zookeeper.quorum", loader.getString("hbase_zookeeper_address"))
    // 创建 Hbase 链接
    val hbConn = ConnectionFactory.createConnection(hadoopConf)
    val hbAdmin = hbConn.getAdmin
    val tbName = TableName.valueOf(hbaseTbName)
    if (!hbAdmin.tableExists(tbName)) {
      println("Table '" + s"$hbaseTbName" + "' is not exists, table has been created.")
      // 创建表描述对象
      val tbDesc = new HTableDescriptor(tbName)
      // 列簇描述
      val colFamilyDesc = new HColumnDescriptor(columnFamily)
      // 加载列簇到表
      tbDesc.addFamily(colFamilyDesc)
      // 创建表
      hbAdmin.createTable(tbDesc)
      hbAdmin.close()
      hbConn.close()
    }
    val jobConf = new JobConf(hadoopConf)
    // 指定Key的输出类型
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    // 指定输出到哪张表
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, hbaseTbName)
    jobConf
  }
}
