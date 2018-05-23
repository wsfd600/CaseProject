package com.lyn.wash

import java.util.UUID

import com.lyn.conf.Config
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{HBaseAdmin, HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer


object StreamingParse {

  //判断值是否为空
  def judgeField1(data: Any):Boolean = {
    if(data != "null" &&  data != " " && data != null){
      true
    }else{
      false
    }
  }
  def writeDate1(data: (String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String), myTable: HTable) = {
    val uuid = UUID.randomUUID()
    val p = new Put(Bytes.toBytes(uuid.toString))
    if(judgeField1(data._1)) p.add("match".getBytes, "caseNumber".getBytes, Bytes.toBytes(data._1.toString))
    if(judgeField1(data._2)) p.add("match".getBytes, "court".getBytes, Bytes.toBytes(data._2.toString))
    if(judgeField1(data._3)) p.add("match".getBytes, "title".getBytes, Bytes.toBytes(data._3.toString))
    if(judgeField1(data._4)) p.add("other".getBytes, "sourceUrl".getBytes, Bytes.toBytes(data._4.toString))
    if(judgeField1(data._5)) p.add("other".getBytes, "judgementType".getBytes, Bytes.toBytes(data._5.toString))
    if(judgeField1(data._6)) p.add("other".getBytes, "judgementDate".getBytes, Bytes.toBytes(data._6.toString))
    if(judgeField1(data._7)) p.add("other".getBytes, "publishDate".getBytes, Bytes.toBytes(data._7.toString))
    if(judgeField1(data._8)) p.add("other".getBytes, "crawlDate".getBytes, Bytes.toBytes(data._8.toString))
    if(judgeField1(data._9)) p.add("other".getBytes, "caseType".getBytes, Bytes.toBytes(data._9.toString))
    if(judgeField1(data._10)) p.add("other".getBytes, "status".getBytes, Bytes.toBytes(data._10.toString))
    if(judgeField1(data._11)) p.add("other".getBytes, "content".getBytes, Bytes.toBytes(data._11.toString))
    if(judgeField1(data._12)) p.add("match".getBytes, "reason".getBytes, Bytes.toBytes(data._12.toString))
    if(judgeField1(data._13)) p.add("other".getBytes, "trialRound".getBytes, Bytes.toBytes(data._13.toString))
    if(judgeField1(data._14)) p.add("other".getBytes, "sourceName".getBytes, Bytes.toBytes(data._14.toString))
    if(judgeField1(data._15)) p.add("other".getBytes, "relateInfo".getBytes, Bytes.toBytes(data._15.toString))
    if(judgeField1(data._16)) p.add("other".getBytes, "legalBase".getBytes, Bytes.toBytes(data._16.toString))
    if(judgeField1(data._17)) p.add("other".getBytes, "relateFile".getBytes, Bytes.toBytes(data._17.toString))
    if(judgeField1(data._18)) p.add("other".getBytes, "caseInfo".getBytes, Bytes.toBytes(data._18.toString))
    myTable.put(p)
  }

  def saveHbase(rdd3: RDD[(String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)]): Unit = {
    createTable(Config.peo, Config.myConf)
    rdd3.foreachPartition(rdd =>{
      rdd.foreach(data => {
        try {
          writeDate1(data, Config.peoTable)
          Config.peoTable.flushCommits()
        } catch {
          case e: Exception =>
            e.printStackTrace()
        }
      })
    })
  }

  def process(rdd: RDD[(String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)]) = {

       //对rdd自身去重
        val combinRdd = rdd.map(f => {
          (f._1+"_"+f._2,(f._3+"|"+f._4+"|"+f._5+"|"+f._6+"|"+f._7+"|"+f._8+"|"+f._9+"|"+f._10.toInt+"|"+f._11+"|"+f._12+"|"+f._13+"|"+f._14+"|"+f._15+"|"+f._16+"|"+f._17+"|"+f._18))
        }).map(x => (x._1,(x._2.toString(),1))).combineByKey(
          (v) => (v),
          (acc: (String,Int), v) => (v._1+"&&"+acc._1,v._2+acc._2),
          (acc1: (String,Int), acc2: (String,Int)) => (acc1._1 +"&&"+ acc2._1, acc1._2 +acc2._2)
           )

        //不重复的数据
         val noRepetaRdd = combinRdd.map(x => {
         val res = x._1.split("_")
         val caseNumber = res(0)
         val court = res(1)
         val values = if(x._2._2 != 1){
           val s = x._2._1.split("&&")
           s(1)
         }else{
           x._2._1
         }
         val value = values.split('|')
         val title = value(0)
         val sourceUrl = value(1)
         val judgementType = value(2)
         val judgementDate = value(3)
         val publishDate = value(4)
         val crawlDate = value(5)
         val caseType = value(6)
         val status = value(7)
         val content = value(8)
         val reason= value(9)
         val trialRound = value(10)
         val sourceName = value(11)
         val relateInfo = value(12)
         val legalBase = value(13)
         val relateFile = value(14)
         val caseInfo = value(15)
      (caseNumber,court,title,sourceUrl,judgementType,judgementDate,publishDate,crawlDate,caseType,status,content,reason,trialRound,sourceName,relateInfo,legalBase,relateFile,caseInfo)
    })

    //重复的数据
    val repetaRdd = combinRdd.filter(x => x._2._2 != 1).foreach(x => {
        createTable(Config.repeteTable, Config.myConf)
        val valueList = x._2._1.split("&&")
        for(i <- 1 to (valueList.length - 1)){
          val values = valueList(i)
          val res = x._1.split("_")
          val caseNumber = res(0)
          val court = res(1)
          val value = values.split('|')
          val title = value(0)
          val sourceUrl = value(1)
          val judgementType = value(2)
          val judgementDate = value(3)
          val publishDate = value(4)
          val crawlDate = value(5)
          val caseType = value(6)
          val status = value(7)
          val content = value(8)
          val reason= value(9)
          val trialRound = value(10)
          val sourceName = value(11)
          val relateInfo = value(12)
          val legalBase = value(13)
          val relateFile = value(14)
          val caseInfo = value(15)
          val data = (caseNumber,court,title,sourceUrl,judgementType,judgementDate,publishDate,crawlDate,caseType,status,content,reason,trialRound,sourceName,relateInfo,legalBase,relateFile,caseInfo)
          try {
            writeDate1(data, Config.repeteTables)
            Config.repeteTables.flushCommits()
          } catch {
            case e: Exception =>
              e.printStackTrace()
          }
        }
    })

        val schema: StructType = StructType {
          Array(
            StructField("caseNumber", StringType, false),
            StructField("court", StringType, true),
            StructField("title", StringType, true),
            StructField("sourceUrl", StringType, true),
            StructField("judgementType", StringType, true),
            StructField("judgementDate", StringType, true),
            StructField("publishDate", StringType, true),
            StructField("crawlDate", StringType, true),
            StructField("caseType", StringType, true),
            StructField("status", IntegerType, true),
            StructField("content", StringType, true),
            StructField("reason", StringType, true),
            StructField("trialRound", StringType, true),
            StructField("sourceName", StringType, true),
            StructField("relateInfo", StringType, true),
            StructField("legalBase", StringType, true),
            StructField("relateFile", StringType, true),
            StructField("caseInfo", StringType, true)
          )
        }
        val sqlContext = Config.sqlContext
        import sqlContext.implicits._

        //把noRepetaRdd映射到rowRDD 和数据库已有数据对比看是否重复
        val rowRDD = noRepetaRdd.map(f => Row(f._1, f._2, f._3, f._4, f._5,f._6,f._7,f._8,f._9,f._10.toInt,f._11,f._12,f._13,f._14,f._15,f._16,f._17,f._18))
        val caseDataFrame = sqlContext.createDataFrame(rowRDD, schema)
        caseDataFrame.registerTempTable("caseWash")

        //从hbase读已有案列,并转成df
        val repete_case = Config.hBaseRDD.map(r => (
          Bytes.toString(r._2.getValue(Bytes.toBytes("match"),Bytes.toBytes("caseNumber")))/*,
          Bytes.toString(r._2.getValue(Bytes.toBytes("match"),Bytes.toBytes("court")))*/
        )).toDF("caseNumber")
        // 测试
        repete_case.registerTempTable("repete_case")
        val df2 = sqlContext.sql("SELECT * FROM repete_case").show()

        //把清洗好的数据和已有的数据联合查询
        //重复数据
        val df4 = sqlContext.sql("SELECT a.*,b.* FROM caseWash a left join repete_case b on b.caseNumber = a.caseNumber and a.court = b.court WHERE b.caseNumber IS NOT NULL and b.court IS NOT NULL")
        df4.foreachPartition(part => {
          part.foreach(data => {
            try {
              createTable(Config.repeteTable, Config.myConf)
              writeDate(data, Config.repeteTables)
              Config.repeteTables.flushCommits()
            } catch {
            case e: Exception =>
              e.printStackTrace()
          }
        })
        })

        //不重复数据
        val df3 = sqlContext.sql("SELECT a.*,b.* FROM caseWash a left join repete_case b on a.caseNumber = b.caseNumber  and a.court = b.court where b.caseNumber is null and b.court is null")
         df3.foreachPartition(part => {
             part.foreach(data => {
               try {
                 writeDate(data, Config.myTable)
                 Config.myTable.flushCommits()
               } catch {
             case e: Exception =>
               e.printStackTrace()
           }
             })
         })
    }

  //caseNumber,court,title,sourceUrl,judgementType,publishDate,crawlDate,content,caseType,status
  //向hbase写数据
  def writeDate(content: Row, myTable: HTable) = {
    val uuid = UUID.randomUUID()
    val p = new Put(Bytes.toBytes(uuid.toString))
    if(judgeField(content,0)) p.add("match".getBytes, "caseNumber".getBytes, Bytes.toBytes(content.get(0).toString))
    if(judgeField(content,1)) p.add("match".getBytes, "court".getBytes, Bytes.toBytes(content.get(1).toString))
    if(judgeField(content,2)) p.add("match".getBytes, "title".getBytes, Bytes.toBytes(content.get(2).toString))
    if(judgeField(content,3)) p.add("other".getBytes, "sourceUrl".getBytes, Bytes.toBytes(content.get(3).toString))
    if(judgeField(content,4)) p.add("other".getBytes, "judgementType".getBytes, Bytes.toBytes(content.get(4).toString))
    if(judgeField(content,5)) p.add("other".getBytes, "judgementDate".getBytes, Bytes.toBytes(content.get(5).toString))
    if(judgeField(content,6)) p.add("other".getBytes, "publishDate".getBytes, Bytes.toBytes(content.get(6).toString))
    if(judgeField(content,7)) p.add("other".getBytes, "crawlDate".getBytes, Bytes.toBytes(content.get(7).toString))
    if(judgeField(content,8)) p.add("other".getBytes, "caseType".getBytes, Bytes.toBytes(content.get(8).toString))
    if(judgeField(content,9)) p.add("other".getBytes, "status".getBytes, Bytes.toBytes(content.get(9).toString.toInt))
    if(judgeField(content,10)) p.add("other".getBytes, "content".getBytes, Bytes.toBytes(content.get(10).toString))
    if(judgeField(content,11)) p.add("match".getBytes, "reason".getBytes, Bytes.toBytes(content.get(11).toString))
    if(judgeField(content,12)) p.add("other".getBytes, "trialRound".getBytes, Bytes.toBytes(content.get(12).toString))
    if(judgeField(content,13)) p.add("other".getBytes, "sourceName".getBytes, Bytes.toBytes(content.get(13).toString))
    if(judgeField(content,14)) p.add("other".getBytes, "relateInfo".getBytes, Bytes.toBytes(content.get(14).toString))
    if(judgeField(content,15)) p.add("other".getBytes, "legalBase".getBytes, Bytes.toBytes(content.get(15).toString))
    if(judgeField(content,16)) p.add("other".getBytes, "relateFile".getBytes, Bytes.toBytes(content.get(16).toString))
    if(judgeField(content,17)) p.add("other".getBytes, "caseInfo".getBytes, Bytes.toBytes(content.get(17).toString))
    myTable.put(p)
  }


  //判断值是否为空
  def judgeField(content: Row,index: Int):Boolean = {
    if(content.get(index) != "null" &&  content.get(index) != " " && content.get(index) != null){
      true
    }else{
      false
    }
  }

  //创建表
  def createTable(tableName: String, myConf: Configuration) = {
    val hBaseAdmin = new HBaseAdmin(myConf)
    if (!hBaseAdmin.tableExists(tableName)) { // 不存在则创建表
      val tableDescriptor = new HTableDescriptor(tableName)
      tableDescriptor.addFamily(new HColumnDescriptor("match"))
      tableDescriptor.addFamily(new HColumnDescriptor("other"))
      hBaseAdmin.createTable(tableDescriptor)
    }
    hBaseAdmin.close()
  }

}
