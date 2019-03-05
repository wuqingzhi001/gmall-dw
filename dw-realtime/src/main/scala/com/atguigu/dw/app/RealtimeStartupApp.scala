package com.atguigu.dw.app

import com.alibaba.fastjson.JSON
import com.atguigu.dw.bean.StartUpLog
import com.atguigu.dw.handle.DauHandler
import com.atguigu.dw.utils.MyKafkaUtils
import com.atguigu.gmall.dw.constant.GmallConstants
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}



/**
  * @aythor HeartisTiger
  *         2019-03-04 20:15
  */
object RealtimeStartupApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("gmall2019")
    val sc = new SparkContext(conf)
    val spark = new StreamingContext(sc,Seconds(5))
    val startupStream= MyKafkaUtils.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP,spark)
    //这是5秒内的活跃用户
    val startStream = startupStream.map(_.value()).map(log => {
      val startuplog = JSON.parseObject(log, classOf[StartUpLog])

      startuplog
    })
    DauHandler.calcDau(startStream,sc)
    spark.start()
    spark.awaitTermination()

  }
}
