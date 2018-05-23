package com.lyn.wash

import java.sql.DriverManager
import java.util.UUID

import com.alibaba.fastjson.JSON
import com.lyn.conf.Config
import com.lyn.util.RegexUtil
import kafka.serializer.StringDecoder
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{HBaseAdmin, HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.collection.mutable;

object CaseTest {
  case class Wenshu(caseNumber:String,
                      title:String,
                      court:String,
                      judgementDate:String,
                      reason:String,
                     judgementType:String,
                      publishDate:String,
                     sourceUrl:String,
                      content:String)
  case class CaseWash(court: String, caseNumber: String, caseType: String, judgementType: String, judgementDate: String)

  def main(args: Array[String]): Unit = {
    val sc = Config.sc

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    //用数据库的大量数据测试正则的准确性
   /* val connection =() =>{
      Class.forName("org.postgresql.Driver").newInstance()
      DriverManager.getConnection("jdbc:postgresql://47.94.1.72:5532/spider","spider","spider@2018")
    }
    val jdbcRDD = new JdbcRDD(
      sc,
      connection,
      "SELECT * from wenshuapp where id >= ? AND id <= ?",
      4000,10000,2,
      r =>{
        val title = r.getString(6)
        /*parsingCase(content)*/
        println(title)
        val titles = RegexUtil.regexReason(title).toString
        println(titles+"=============")
      }
    )
 jdbcRDD.count()*/

    //val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    //按照案号和裁决日期去重
    val fileRdd = sc.textFile("hdfs://bdyh2:8020/lyn/bigdata.txt")
    val washRdd = fileRdd.distinct().map(x => {
      val arr = x.split('|')
      val caseNumber = arr(0).replaceAll("（","(").replaceAll("）",")")
      val title = arr(1)
      val court = arr(2)
      //如果案由等于null 匹配案由
      val reason = if (arr(3) == "null" || arr(3) == "" || arr(3) == null) {
        RegexUtil.regexReason(title).toString
      } else {
        arr(3)
      }
      val publishDate = arr(4).replaceAll("-", "")
      val sourceUrl = arr(5)
      val content = arr(6)
      val judgementDate = arr(7).replaceAll("-", "")
      val caseType = RegexUtil.regexCaseType(x)
      (caseNumber,title, court, reason,publishDate,sourceUrl, content,judgementDate)
    })


    sc.stop()
  }

}