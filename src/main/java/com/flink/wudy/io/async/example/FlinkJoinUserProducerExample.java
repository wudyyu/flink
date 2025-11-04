package com.flink.wudy.io.async.example;

import com.alibaba.fastjson.JSON;
import com.flink.wudy.config.KafkaProperties;
import com.flink.wudy.io.async.model.UserClickMsg;
import com.flink.wudy.io.async.source.OrderClickMsgSource;
import com.flink.wudy.io.async.source.UserClickMsgSource;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class FlinkJoinUserProducerExample {
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty("group.id", "flink-join-user-group");
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "3600000");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(ParameterTool.fromArgs(args).getConfiguration());
        env.setParallelism(2);
        env.setMaxParallelism(6);

        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<String>(
                KafkaProperties.USER_SERVER_TOPIC, new SimpleStringSchema(), properties
        );

        DataStreamSource<UserClickMsg> userSource = env.addSource(new UserClickMsgSource());

        userSource.keyBy(new KeySelector<UserClickMsg, Long>() {
            @Override
            public Long getKey(UserClickMsg userClickMsg) throws Exception {
                return userClickMsg.getUserId();
            }
        }).map(new RichMapFunction<UserClickMsg, String>() {
            @Override
            public String map(UserClickMsg userClickMsg) throws Exception {
                return JSON.toJSONString(userClickMsg);
            }
        }).addSink(producer).name("写入kafka user-server-topic");

        FlinkKafkaProducer<String> orderProducer = new FlinkKafkaProducer<String>(
                KafkaProperties.ORDER_SERVER_TOPIC, new SimpleStringSchema(), properties
        );

        DataStreamSource<UserClickMsg> orderSource = env.addSource(new OrderClickMsgSource());
        orderSource.keyBy(new KeySelector<UserClickMsg, Long>() {
            @Override
            public Long getKey(UserClickMsg userClickMsg) throws Exception {
                return userClickMsg.getUserId();
            }
        }).map(new RichMapFunction<UserClickMsg, String>() {
            @Override
            public String map(UserClickMsg userClickMsg) throws Exception {
                return JSON.toJSONString(userClickMsg);
            }
        }).addSink(orderProducer).name("写入kafka order-server-topic");

        env.execute("Add User And Order ClickMsg");
    }
}
