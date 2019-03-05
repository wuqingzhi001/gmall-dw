package com.atguigu.dw.canal.client

import com.alibaba.fastjson.JSONObject
import com.alibaba.google.common.base.CaseFormat
import com.alibaba.otter.canal.protocol.CanalEntry

import scala.collection.mutable

/**
  * @aythor HeartisTiger
  *         2019-03-05 15:01
  */
object Handle {
  def handleEvent(tableName:String,eventType:CanalEntry.EventType ,rows :mutable.ListBuffer[CanalEntry.RowData])={
    if("order_info".equals(tableName)&&CanalEntry.EventType.INSERT==eventType){
      implicit  import scala.collection.JavaConverters._
      for (row <- rows) {

        val columns = row.getAfterColumnsList.asScala

        var jsonObj = new JSONObject()
        for (col <- columns) {
          var colName = col.getName
          var colValue = col.getValue

          jsonObj.put(CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,colName))
        }
      }
    }
  }
}
