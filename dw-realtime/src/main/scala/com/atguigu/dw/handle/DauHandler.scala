package com.atguigu.dw.handle

import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.dw.bean.StartUpLog
import com.atguigu.dw.utils.MyEsUtils
import com.atguigu.gmall.dw.utils.PropertiesUtil
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
  * @aythor HeartisTiger
  *         2019-03-04 20:43
  */
object DauHandler {
  def calcDau(startupLogDstream:DStream[StartUpLog],sc:SparkContext)= {
    val properties = PropertiesUtil.load("config.properties")
    val jedisDriver = new Jedis(properties.getProperty("redis.host"), properties.getProperty("redis.port").toInt)
    val dateString: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())


    val dauKey = "dau:" + dateString

    val filterDStream = startupLogDstream.transform {
      rdd =>
        val dauSet = jedisDriver.smembers(dauKey)
        val dauBC = sc.broadcast(dauSet)
        println("" + rdd.count())
        val filterRDD = rdd.filter(startupLog =>
          !dauBC.value.contains(startupLog.mid)
        )
        filterRDD

    }
    //redis
    filterDStream.foreachRDD { rdd =>
      rdd.foreachPartition { logItr =>
        val jedisExecutor = new Jedis(properties.getProperty("redis.host"), properties.getProperty("redis.port").toInt)
        val listbuffer = new ListBuffer[Any]()
        for (startUpLog <- logItr) {
          if (!jedisExecutor.sismember(dauKey, startUpLog.mid)) {
            jedisExecutor.sadd(dauKey, startUpLog.mid)
            startUpLog.logDate = new SimpleDateFormat("yyyy-MM-dd").format(new Date(startUpLog.ts))
            startUpLog.logHourMinute = new SimpleDateFormat("HH:mm").format(new Date(startUpLog.ts))
            startUpLog.logHour = startUpLog.logHourMinute.split(":")(0)
            startUpLog
            listbuffer+= startUpLog

          }
          import com.atguigu.gmall.dw.constant.GmallConstants
          MyEsUtils.executeIndexBulk(GmallConstants.ES_INDEX_DAU, listbuffer.toList, null)

        }
      }
      filterDStream
    }

  }
}
