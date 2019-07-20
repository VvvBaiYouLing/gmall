package com.atguigu.logger;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.constant.GmallConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;


@Slf4j
@Controller
public class LogHandler {

    @Autowired
    KafkaTemplate<String,String> kafkaTemplate;


    @ResponseBody
    @PostMapping("log")
    public String doLog(@RequestParam("logString") String logString) {

        // 0 补充时间戳
        JSONObject jsonObject = JSON.parseObject(logString);
        jsonObject.put("ts", System.currentTimeMillis());
        // 1 落盘 file
        String jsonString = jsonObject.toJSONString();
        log.info(jsonObject.toJSONString());


        // 2 推送到kafka
        if ("startup".equals(jsonObject.getString("type"))) {
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_STARTUP, jsonString);
        } else {
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_EVENT, jsonString);
        }

        return "success";


    }

    @ResponseBody
    @RequestMapping("/test")
    public String test() {

        return "test";
    }

}
