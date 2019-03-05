package com.atguigu.dw.canal.client

import java.net.InetSocketAddress

import com.alibaba.otter.canal.client.{CanalConnector, CanalConnectors}
import com.alibaba.otter.canal.protocol.CanalEntry
import com.alibaba.otter.canal.protocol.CanalEntry.{EntryType, RowChange}

import scala.util.control.Breaks

/**
  * @aythor HeartisTiger
  *         2019-03-05 14:17
  */
object CanalClient {
  def main(args: Array[String]): Unit = {
    var canal = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop101",11111),)

    while(true){
      //尝试建立 canal服务器
      canal.connect()
      //订阅 数据表
      canal.subscribe("gmall0901.order_info")
      //获取发生的sql 得到message
      val messages = canal.get(100)

      println("获取sql"+messages.getEntries)

      if(messages.getEntries.size() ==0){
        println("无事发生")
        try {
          Thread.sleep(5000)
        }catch { case e :Exception =>e.printStackTrace()}
      } else{
        import scala.collection.JavaConverters._
        import util.control.Breaks._
        val entries = messages.getEntries.asScala

        for(entry <- entries){
          breakable {
            if (entry.getEntryType.equals(EntryType.TRANSACTIONBEGIN) || entry.getEntryType.equals(EntryType.TRANSACTIONEND)) {
              break()
            }
          }
          var rowChange = null
          RowChange.parseFrom(entry.getStoreValue)

        }
      }




    }
  }
}
