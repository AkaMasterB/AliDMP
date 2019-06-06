package com.tags

import com.common.UserMaching
import com.typesafe.config.ConfigFactory
import com.utils.JedisConnectionPool
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * ······················最终版本·······················
  * 功能描述:
  * 〈 上下文标签（整合标签项），借助spark图计算来统一用户〉
  *
  * @since: 1.0.0
  * @Author:SiXiang
  * @Date: 2019/6/6 0:12
  */
object ContextTagsV2 {
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
      .map((_, 0))
      .collect()
      .toMap
    val globalStopWord = sc.broadcast(stopWordMap)

    // 初始化hbase、创建表
    //    val jobConf = initHtable(sc, "tags")


    // 获取所有用户字段(不同方式记录)
    val baseRDD = sQLContext.read.parquet(inputPath)
      .filter(UserMaching.hasNeedOneUserId)
      .map(row => {
        val allUserId = UserMaching.getUserIdAll(row)
        (allUserId, row)
      })
    // 构建graph点集合
    val vertices: RDD[(Long, List[(String, Int)])] = baseRDD
      .mapPartitions(partition => {
        val jedis = JedisConnectionPool.getConnection()
        val newPartition: Iterator[(List[String], Row, List[(String, Int)])] = partition
          .map(t => {
            val bAreaTag = BusinessAreaTag.makeTags(t._2, jedis)
            (t._1, t._2, bAreaTag)
          })
        jedis.close()
        newPartition
      })
      .flatMap(tp => {
        val row = tp._2
        val adSpaceTypeTag = AdSpaceTypeTag.makeTags(row)
        val appTag = AppTag.makeTags(row, globalDict)
        val adPlatformTag = AdPlatformProviderTag.makeTags(row)
        val deviceTag = DeviceTag.makeTags(row)
        val kwdTag = KeyWordTag.makeTags(row, globalStopWord)
        val locTag = LocationTag.makeTags(row)
        // 把所有的标签放到一起形成一个集合
        val totalList = adSpaceTypeTag ++ appTag ++ adPlatformTag ++ deviceTag ++ kwdTag ++ locTag ++ tp._3
        // 匹配格式，拼接 UserId-List 和 Tags-List
        val target = tp._1.map((_, 0)) ++ totalList
        // 保证一个Id携带者标签，其他的Id的value都是空的集合
        tp._1.map(uId => {
          if (tp._1.head.equals(uId)) {
            (uId.hashCode.toLong, target)
          } else {
            (uId.hashCode.toLong, List.empty)
          }
        })
      })


    // 构建graph边集合
    val edges = baseRDD.flatMap(tp => {
      tp._1.map(uId => {
        Edge(tp._1.head.hashCode, uId.hashCode.toLong, 0)
      })
    })

    // 构建图
    val graph = Graph(vertices, edges)
    // 顶点
    val cc: VertexRDD[VertexId] = graph.connectedComponents().vertices
    // 合并归一
    cc.join(vertices)
      .map {
        case (uId, (comm, tagsAndUid)) => (comm, tagsAndUid)
      }
      .reduceByKey(
        (list1, list2) => list1 ::: list2
          .groupBy(_._1)
          .mapValues(_.size)
          .toList
      )
      .foreach(println)
    //      .map {
    //        case (userId, userTags) => {
    //          // rowkey
    //          val put = new Put(Bytes.toBytes(userId))
    //          // data
    //          val tags = userTags.map(t => t._1 + "," + t._2).mkString(";")
    //          // 列簇--列--数据
    //          put.addImmutable(Bytes.toBytes("tags"), Bytes.toBytes(s"$date"), Bytes.toBytes(tags))
    //          (new ImmutableBytesWritable(), put)
    //        }
    //      }
    //      .saveAsHadoopDataset(jobConf)
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
