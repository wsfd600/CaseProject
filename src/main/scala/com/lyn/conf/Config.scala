package com.lyn.conf

import java.util.UUID

import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import org.apache.hadoop.hbase.client.{HTable, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by lyn on 2018/4/22.
  */
object Config extends Serializable{

  val isLocal=true//是否使用local模式
  val firstReadLastest=true//第一次启动是否从最新的开始消费

  val sparkConf=new SparkConf().setAppName("DataDeduplication")
  if (isLocal)  sparkConf.setMaster("local[1]") //local模式
  sparkConf.set("spark.streaming.stopGracefullyOnShutdown","true")//优雅的关闭
  sparkConf.set("spark.streaming.backpressure.enabled","true")//激活削峰功能
  sparkConf.set("spark.streaming.backpressure.initialRate","3000")//第一次读取的最大数据值
  sparkConf.set("spark.streaming.kafka.maxRatePerPartition","1000")//每个进程每秒最多从kafka读取的数据条数

  @transient
  val sc = new SparkContext(sparkConf)

  var kafkaParams=Map[String,String]("metadata.broker.list"-> "bdyh1:9092,bdyh2:9092,bdyh3:9092,bdyh4:9092,bdyh5:9092")//创建一个kafkaParams
  kafkaParams += ("serializer.class" -> "kafka.serializer.StringEncoder")
  if (firstReadLastest)   kafkaParams += ("auto.offset.reset"-> "smallest")//从最旧的数据开始消费
  val zk = "bdyh1:2181,bdyh2:2181,bdyh3:2181,bdyh4:2181,bdyh5:2181"
  //创建zkClient注意最后一个参数最好是ZKStringSerializer类型的，不然写进去zk里面的偏移量是乱码
  //val  zkClient= new ZkClient("bdyh1:2181,bdyh2:2181,bdyh3:2181,bdyh4:2181,bdyh5:2181", 30000, 30000,ZKStringSerializer)
  val group = "group1"
  val zkOffsetPath="/spider"//zk的路径
  val topic = "spider"
  val topicsSet="spider".split(",").toSet//topic名字
  val brokerList = "bdyh1:9092,bdyh2:9092,bdyh3:9092,bdyh4:9092,bdyh5:9092"
  val zkQuorum="bdyh1:2181,bdyh2:2181,bdyh3:2181,bdyh4:2181,bdyh5:2181"
  @transient
  val ssc=new StreamingContext(sc,Seconds(5))//创建StreamingContext,每隔多少秒一个批次

  val sqlContext = new SQLContext(sc)

  //hbase表名
  val tableName = "spider" //案例表
  val repeteTable = "repedata" //重复表
  val peo = "peodata" //人工去重表

  val myConf = HBaseConfiguration.create()
  myConf.set("hbase.zookeeper.quorum", "bdyh1,bdyh2,bdyh3,bdyh4,bdyh5")
  myConf.set("hbase.zookeeper.property.clientPort", "2181")
  myConf.set("hbase.defaults.for.version.skip", "true")
  myConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  myConf.set(TableInputFormat.INPUT_TABLE, tableName)

  val myTable = new HTable(myConf, TableName.valueOf(tableName))
  myTable.setAutoFlush(false, false)//关键点1
  myTable.setWriteBufferSize(3*1024*1024)//关键点2

  val peoTable = new HTable(myConf, TableName.valueOf(peo))
  peoTable.setAutoFlush(false, false)//关键点1
  peoTable.setWriteBufferSize(3*1024*1024)//关键点2

  val repeteTables = new HTable(myConf, TableName.valueOf(repeteTable))
  repeteTables .setAutoFlush(false, false)//关键点1
  repeteTables .setWriteBufferSize(3*1024*1024)//关键点2

  val hBaseRDD = sc.newAPIHadoopRDD(myConf, classOf[TableInputFormat],
    classOf[ImmutableBytesWritable],
    classOf[Result])

  val uuid = UUID.randomUUID()
}
