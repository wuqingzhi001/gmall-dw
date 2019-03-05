package com.atguigu.gmall.dw.utils

import java.io.InputStreamReader
import java.util.Properties

/**
  * @aythor HeartisTiger
  *         2019-02-16 14:14
  */
object PropertiesUtil {
  def load(propertieName:String): Properties ={
    val prop=new Properties();
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName) , "UTF-8"))
    prop
  }

}
