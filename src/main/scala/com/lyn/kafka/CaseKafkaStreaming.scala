package com.lyn.kafka

import com.alibaba.fastjson.{JSON, JSONObject}
import com.lyn.conf.Config
import com.lyn.util.LineJudgeUtil
import com.lyn.wash.StreamingParse
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.apache.spark.SparkConf
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

  /**
    *
    */
  object CaseKafkaStreaming {

    def main(args: Array[String]) {

      val group ="g10001"
      val conf = Config.sparkConf
      val ssc = Config.ssc
      val topic = Config.topic

      val topics:Set[String]=Set(topic)
      val topicDirs = new ZKGroupTopicDirs(group,topic)
      val zkTopicPath = s"${topicDirs.consumerOffsetDir}"
      println("zkTopicPath is:" +zkTopicPath)
      val kafkaParams = Config.kafkaParams
      val zkClient = new ZkClient(Config.zkQuorum)
      val s = zkClient.exists(zkTopicPath)
      println("s value is :"+s)
      val children = zkClient.countChildren(zkTopicPath)
      println("c value is:" + children)
      var kafkaStream:InputDStream[(String,String)] = null
      var fromOffsets:Map[TopicAndPartition,Long]=Map()
      if(children>0){
        for(i <- 0 until children){
          val partitionOffset = zkClient.readData[String](s"${topicDirs.consumerOffsetDir}/${i}")
          val tp =TopicAndPartition(topic,i)
          fromOffsets += (tp -> partitionOffset.toLong)
        }
        val messageHandler = (mmd:MessageAndMetadata[String,String]) =>(mmd.topic,mmd.message())
        kafkaStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder,(String,String)](ssc,kafkaParams,fromOffsets,messageHandler)
      }else{
        kafkaStream=  KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topics)
      }

      var offsetRanges = Array[OffsetRange]()
      kafkaStream.transform(rdd =>{
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }).map(_._2)
        .foreachRDD{rdd =>
          if(!rdd.isEmpty()){
            //只处理有数据的rdd，没有数据的直接跳过
            val filterRDD = rdd.distinct().filter(x => x != null && x.length() != 0 && x!= " " && x.substring(0,1) == "{" && x.substring(x.length - 1,x.length) == "}")
            try {
              //判断键是否存在,存在去除对应的值
              val fileRdd = filterRDD.map(x => {
                val json = JSON.parseObject(x)
                //获取成员
                val ca = KeyIsExist(json,"caseNumber")
                val caseNumb = if (ca != null && ca != "null" && ca != "") ca else KeyIsExist(json,"casenumber")
                val caseNumber = caseNumb.replace(" ", "").replace("[", "(").replace("【", "(").replace("（", "(")
                  .replace("]", ")").replace("　", "").replace("〔", "(").replace("〕", ")").replace("】", ")").replace("）", ")").replace("号)","号" ).replace(" ", "").replace("((", "(");
                val court = KeyIsExist(json,"court")
                val title= KeyIsExist(json,"title")
                val soua = KeyIsExist(json,"sourceUrl")
                val sourceUrl = if(soua != null && soua !="null" && soua != "") soua else KeyIsExist(json,"sourceurl")
                val judea = KeyIsExist(json,"judgementType")
                val judgementType = if(judea != null && judea != "null" && judea != "") judea else KeyIsExist(json,"judgementtype")
                val judga = KeyIsExist(json,"judgementDate")
                val judgementDate = if(judga != null && judga != "null" && judga != "") judga else KeyIsExist(json,"judgementdate")
                val pua = KeyIsExist(json,"publishDate")
                val publishDate = if(pua != null && pua != "null" && pua != "") pua else KeyIsExist(json,"publishdate")
                val cra = KeyIsExist(json,"crawlDate")
                val crawlDate = if(cra != null && cra != "null" && cra != "") cra else KeyIsExist(json,"crawldate")
                val casa = KeyIsExist(json,"caseType")
                val caseType =  if(casa != null && casa != "null" && casa != "") cra else KeyIsExist(json,"casetype")
                val status =  KeyIsExist(json,"status")
                val content = KeyIsExist(json,"content")
                val reason = "null"
                val souraa = KeyIsExist(json,"sourceName")
                val sourceName = if(souraa != null && souraa != "null" && souraa != "") souraa else KeyIsExist(json,"sourcename")
                val trial = KeyIsExist(json,"trialRound")
                val trialRound = if(trial != null && trial != "null" && trial != "") trial else KeyIsExist(json,"trialround")
                val rela  =  KeyIsExist(json,"relateInfo")
                val relateInfo = if(rela != null && rela != "null" && rela != "") rela else KeyIsExist(json,"relateinfo")
                val lega = KeyIsExist(json,"legalBase")
                val legalBase  =  if(lega != null && lega != "null" && lega != "") lega else KeyIsExist(json,"legalbase")
                val relat = KeyIsExist(json,"relateFile")
                val relateFile = if(relat != null && relat != "null" && relat != "") relat else KeyIsExist(json,"relatefile")
                val casei = KeyIsExist(json,"caseInfo")
                val caseInfo = if(casei != null &&casei != "null" && casei != "") casei else KeyIsExist(json,"caseinfo")
                val result =  LineJudgeUtil.judge(caseNumber, court, judgementDate, caseType, content)
                (result,(caseNumber,court,title,sourceUrl,judgementType,judgementDate,publishDate,crawlDate,caseType,status,content,reason,trialRound,sourceName,relateInfo,legalBase,relateFile,caseInfo))
              })
              //正常数据 按照案号 法院文书类型去重
              val rdd1 = fileRdd.filter(x => x._1 == 1).map(_._2)
              StreamingParse.process(rdd1)
              //无案号数据 根据高院 标题裁决日期去重
              val rdd2 = fileRdd.filter(x => x._1 == 2).map(_._2)
              StreamingParse.process(rdd2)
              //需要人工匹配的数据
              val rdd3 = fileRdd.filter(x => x._1 == 3).map(_._2)
              StreamingParse.saveHbase(rdd3)
              for(o <- offsetRanges){
                //log.info("更新偏移量......")
                val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
                //ZkUtils.updatePersistentPath(zkClient,zkPath,o.fromOffset.toString())
                //log.info("偏移量已更新......")
              }

        } catch {
        case e: Exception =>
          e.printStackTrace()
        case e: NullPointerException =>
          e.printStackTrace()
      }
            //更新每个批次的偏移量到zk中，注意这段代码是在driver上执行的
            //KafkaOffsetManager.saveOffsets(Config.zkClient,Config.zkOffsetPath,rdd)
          }else{
            //log.info("rdd is isEmpty......")
          }
        }

      ssc.start()
      ssc.awaitTermination()
    }
    def KeyIsExist(json: JSONObject, fi: Any):String = {
      if (json.containsKey(fi)) {
        json.get(fi).toString
      } else {
        null
      }
    }
}
