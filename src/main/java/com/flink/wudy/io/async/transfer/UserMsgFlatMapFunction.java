package com.flink.wudy.io.async.transfer;

import com.alibaba.druid.support.json.JSONUtils;
import com.alibaba.fastjson.JSON;
import com.flink.wudy.io.async.model.UserClickMsg;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

@Slf4j
public class UserMsgFlatMapFunction implements FlatMapFunction<String, UserClickMsg> {

    @Override
    public void flatMap(String msg, Collector<UserClickMsg> collector) throws Exception {
        try {
            UserClickMsg userMsg = JSON.parseObject(msg, UserClickMsg.class);
            System.out.println(userMsg);
            collector.collect(userMsg);
        }catch (Exception e){
            log.error("msg content error:{}", msg);
        }
    }
}
