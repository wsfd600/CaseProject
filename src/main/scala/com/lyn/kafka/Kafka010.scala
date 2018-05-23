package com.lyn.kafka

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import kafka.message.MessageAndMetadata
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.{ZkClient, ZkConnection}
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.HasOffsetRanges

object Kafka010 {

    def main(args: Array[String]): Unit = {

      //参数获取
      val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> "bdyh1:9092,bdyh2:9092,bdyh3:9092,bdyh4:9092,bdyh5:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "group1",
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (false: java.lang.Boolean)
      )
      val topic: String = "spider"
      val topics: Set[String] = Set(topic)

      //初始化驱动
      val conf = new SparkConf().setMaster("local[8]").setAppName("TransformOPer")
      val sc = new SparkContext(conf)
      val ssc = new StreamingContext(sc, Seconds(5))


      //保存偏移量
      val zkHosts = "bdyh5:2181,bdyh4:2181,bdyh3:2181,bdyh2:2181,bdyh1:2181"
      val zkClient = new ZkClient(zkHosts, 10000, 10000, new ZkSerializer {
        override def serialize(data: scala.Any): Array[Byte] = data.asInstanceOf[String].getBytes("UTF-8")

        override def deserialize(bytes: Array[Byte]): AnyRef = if (bytes == null) null else new String(bytes, "UTF-8")
    })
      val zkUtils = new ZkUtils(zkClient, new ZkConnection(zkHosts), false)
      val topicDirs = new ZKGroupTopicDirs("spider", topic)
      val children = zkClient.countChildren(s"${topicDirs.consumerOffsetDir}")

      var fromOffsets: Map[TopicPartition, Long] = Map()

      val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())

      var stream: InputDStream[ConsumerRecord[String, String]]  = null
      if (children > 0) {


        for (i <- 0 until children) {
          val partitionOffset = zkClient.readData[String](s"${topicDirs.consumerOffsetDir}/$i")
          val tp = new TopicPartition(topic, i)
          fromOffsets += (tp -> partitionOffset.toLong)

        }
        stream = org.apache.spark.streaming.kafka010.KafkaUtils.createDirectStream[String, String](
          ssc,
          PreferConsistent,
          Assign[String, String](fromOffsets.keys.toList, kafkaParams, fromOffsets)
        )
      }
      else {
        stream = org.apache.spark.streaming.kafka010.KafkaUtils.createDirectStream[String, String](
          ssc,
          PreferConsistent,
          Subscribe[String, String](topics, kafkaParams)
        )

      }

      //处理数据
      stream.foreachRDD(rdd => {
        rdd.foreachPartition(aa => {
          aa.foreach(println)

        })
        //处理成功后获取偏移量
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        for (o <- offsetRanges) {
          val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
          zkUtils.updatePersistentPath(zkPath, o.fromOffset.toString)  //将该 partition 的 offset 保存到 zookeeper
          println(s"@@@@@@ topic  ${o.topic}  partition ${o.partition}  fromoffset ${o.fromOffset}  untiloffset ${o.untilOffset} #######")
        }
      })

      //启动程序
      //ssc.checkpoint("/tmp/checkpoint")
      ssc.start()
      ssc.awaitTermination()

    }

}
