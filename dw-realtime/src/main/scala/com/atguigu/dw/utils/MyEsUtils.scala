package com.atguigu.dw.utils

/**
  * @aythor HeartisTiger
  *         2019-03-04 22:56
  */
import java.util.Objects

import com.google.gson.GsonBuilder
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index}
import org.apache.commons.beanutils.BeanUtils

object MyEsUtils {
  private val ES_HOST = "http://hadoop101"
  private val ES_HTTP_PORT = 9200
  private var factory:JestClientFactory = null

  /**
    * 获取客户端
    *
    * @return jestclient
    */
  def getClient: JestClient = {
    if (factory == null) build()
    factory.getObject
  }

  /**
    * 关闭客户端
    */
  def close(client: JestClient): Unit = {
    if (!Objects.isNull(client)) try
      client.close()
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  /**
    * 建立连接
    */
  private def build(): Unit = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_HOST + ":" + ES_HTTP_PORT).multiThreaded(true)
      .maxTotalConnection(20) //连接总数
      .connTimeout(10000).readTimeout(10000).build)

  }


  def executeIndexBulk(indexName:String ,list:List[Any], idColumn:String): Unit ={
    val bulkBuilder: Bulk.Builder = new Bulk.Builder().defaultIndex(indexName).defaultType("_doc")
    for ( doc <- list ) {

      val indexBuilder = new Index.Builder(doc)
      if(idColumn!=null){
        val id: String = BeanUtils.getProperty(doc,idColumn)
        indexBuilder.id(id)
      }
      val index: Index = indexBuilder.build()
      bulkBuilder.addAction(index)
    }
    val jestclient: JestClient =  getClient

    val result: BulkResult = jestclient.execute(bulkBuilder.build())
    if(result.isSucceeded){
      println("保存成功:"+result.getItems.size())
    }

  }
}
