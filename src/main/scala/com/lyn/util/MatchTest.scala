package com.lyn.util

import java.sql.DriverManager
import java.util.UUID

import com.alibaba.fastjson.{JSON, JSONObject}
import com.lyn.conf.Config
import com.lyn.wash.StreamingParse
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
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.collection.mutable;

object MatchTest {
  case class CaseWash(court:String,caseNumber:String,caseType:String,judgementType:String,judgementDate:String)

  def main(args: Array[String]): Unit = {
   val sc = Config.sc

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    //用数据库的大量数据测试正则的准确性
    /* val connection =() =>{
     Class.forName("com.mysql.jdbc.Driver").newInstance()
     DriverManager.getConnection("jdbc:mysql://47.94.105.138:3306/spider","root","ljbpyshgcssjcs@2017")
   }
   val jdbcRDD = new JdbcRDD(
     sc,
     connection,
     "SELECT * from cqWenshu where id >= ? AND id <= ?",
     1,200,2,
     r =>{
       val content = r.getString(13)
       parsingCase(content)
     }
   )
   jdbcRDD.count()*/

  val fileRdd = sc.textFile("测试数据.txt").map { file => {
      //解析数据
      val json = JSON.parseObject(file)
    //获取成员
    val caseNumber = KeyIsExist(json,"caseNumber").replace(" ", "").replace("[", "(").replace("【", "(").replace("（", "(")
      .replace("]", ")").replace("　", "").replace("〔", "(").replace("〕", ")").replace("】", ")").replace("）", ")").replace("号)","号" ).replace(" ", "").replace("((", "(")
    val court = KeyIsExist(json,"court")
    val title= KeyIsExist(json,"title")
    val sourceUrl = KeyIsExist(json,"sourceUrl")
    val judgementType = KeyIsExist(json,"judgementType")
    val judgementDate = KeyIsExist(json,"judgementDate")
    val publishDate = KeyIsExist(json,"publishDate")
    val crawlDate = KeyIsExist(json,"crawlDate")
    val caseType =  KeyIsExist(json,"caseType")
    val status =  KeyIsExist(json,"status")
    val content = KeyIsExist(json,"content")
    val reason = "null"
    val trialRound = KeyIsExist(json,"trialRound")
    val sourceName =  KeyIsExist(json,"sourceName")
    val relateInfo  =  KeyIsExist(json,"relateInfo")
    val legalBase  =  KeyIsExist(json,"legalBase")
    val relateFile = KeyIsExist(json,"relateFile")
    val caseInfo  =  KeyIsExist(json,"caseInfo")
    val result =  LineJudgeUtil.judge(caseNumber, court, judgementDate, caseType, content)
    (result,(caseNumber,court,title,sourceUrl,judgementType,judgementDate,publishDate,crawlDate,caseType,status,content,reason,trialRound,sourceName,relateInfo,legalBase,relateFile,caseInfo))
    }
    }

    //正常数据 按照案号 法院文书类型去重
    val rdd1 = fileRdd.filter(x => x._1 == 1).map(_._2)
    StreamingParse.process(rdd1)
    //无案号数据 根据高院 标题裁决日期去重
    val rdd2 = fileRdd.filter(x => x._1 == 2).map(_._2)
    StreamingParse.process(rdd2)
    //需要人工匹配的数据
    val rdd3 = fileRdd.filter(x => x._1 == 3).map(_._2)
    StreamingParse.saveHbase(rdd3)
}
  def KeyIsExist(json: JSONObject, fi: Any):String = {
    if (json.containsKey(fi)) {
      json.get(fi).toString
    } else {
      null
    }
  }
}