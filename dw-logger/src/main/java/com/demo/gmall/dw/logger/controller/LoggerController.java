package com.demo.gmall.dw.logger.controller;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.dw.constant.GmallConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @aythor HeartisTiger
 * 2019-02-22 19:30
 */
@RestController
public class LoggerController {
    @Autowired
    private KafkaTemplate<String,String> kafka;
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerController.class);
    @PostMapping("/log")
    public void log(@RequestParam("log") String logJson){
    JSONObject jsonObject = JSON.parseObject(logJson);
    jsonObject.put("ts",System.currentTimeMillis());
    sendKafka(jsonObject);
    String newLog = jsonObject.toJSONString();

    LOGGER.info(newLog);
    }
    private  void sendKafka(JSONObject jsonObject){
        if(GmallConstants.LOG_TYPE_STARTUP().equals(jsonObject.getString("type"))){
            kafka.send(GmallConstants.KAFKA_TOPIC_STARTUP(),jsonObject.toJSONString());
        }
        if(GmallConstants.LOG_TYPE_event().equals(jsonObject.getString("type"))){
            kafka.send(GmallConstants.KAFKA_TOPIC_EVENT(),jsonObject.toJSONString());

        }
    }

}
