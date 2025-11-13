package com.flink.wudy.io.async.example;

import com.alibaba.fastjson.JSON;
import com.flink.wudy.config.KafkaProperties;
import com.flink.wudy.handler.AsyncHandler;
import com.flink.wudy.io.async.model.UserClickMsg;
import com.flink.wudy.io.async.transfer.UserMsgFlatMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.lang.module.Configuration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/*
 参考: https://mp.weixin.qq.com/s/7BGb55_7WdHwK0NvFm4n2w
* 异步IO处理 API的注意事项
* 异步IO算子输入数据的顺序(source -> keyby -> map -> flatmap) 与 经过异步IO算子处理后的输出结果的顺序 需要保证一致性?
* >1.产出数据的顺序保障:
*  AsyncDataStream.unorderedWait() 和 AsyncDataStream.orderedWait() 两个方法来构建异步IO处理能力的算子
*
* 有序模式：AsyncDataStream.orderedWait() ,可以保证产出结果数据流中数据的顺序和输入数据流中数据的顺序相同。
*         两条异步请求A、B， 如果B的结果先返回，会将B的结果保存下来，待A的请求结果返回后，才会发出B的计算结果
*         适用场景：对数据产出顺序有严格要求的场景 （例： 创建商品订单，支付订单，订单退款）
*
* 无序模式： AsyncDataStream.unorderedWait()
*         异步IO请求一结束就会立马发出计算结果
*
* */
public class FlinkJoinStreamAsyncExample {
    private static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    private static final Integer DURATION = 60000;

    public static void main(String[] args) throws Exception {
        System.out.println("开启flink程序接收kafka数据源");
        // 周期性触发状态快照:决定多长时间触发一次Checkpoint
        env.enableCheckpointing(DURATION);
        // 设置为精确一次语义:每条数据只会被处理一次，状态和输出都保证精确一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 一次checkpoint完成的最长等待时机，如果超时，该checkpoint会被丢弃
        env.getCheckpointConfig().setCheckpointTimeout(DURATION*2);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", KafkaProperties.KAFKA_BROKER_ADDRESS);
        properties.setProperty("group.id", "test_group_id");

        //假设我们有多个topic作为消息入口，可以采用flatmap来对消息格式统一，然后用union将各个流聚合起来
        //        FlinkKafkaConsumer<String> userCenterSource = new FlinkKafkaConsumer<>(KafkaProperties.USER_SERVER_TOPIC, new SimpleStringSchema(), properties);
        //        DataStream<UserClickMsg> userDataStream = env.addSource(userCenterSource).name("用户消息").flatMap(new UserMsgFlatMapFunction());

        KafkaSource<String> userCenterSource = KafkaSource.<String>builder().setBootstrapServers(KafkaProperties.KAFKA_BROKER_ADDRESS).setTopics(KafkaProperties.USER_SERVER_TOPIC).setGroupId(KafkaProperties.USER_SERVER_GROUP_ID)
                .setStartingOffsets(OffsetsInitializer.earliest()).setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("client.id.prefix", "user-center-source") // 添加唯一前缀
                .build();
        SingleOutputStreamOperator<UserClickMsg> userDataStream = env.fromSource(userCenterSource, WatermarkStrategy.noWatermarks(),  "用户消息")
                .flatMap(new UserMsgFlatMapFunction())
                .map(msg -> {
                    System.out.println("用户数据流处理结果: " + msg);
                    return msg;
                })
                .name("用户数据流");

//        FlinkKafkaConsumer<String> orderCenterSource = new FlinkKafkaConsumer<>(KafkaProperties.ORDER_SERVER_TOPIC, new SimpleStringSchema(), properties);
//        DataStream<UserClickMsg> orderDataStream = env.addSource(orderCenterSource).name("订单消息").flatMap(new UserMsgFlatMapFunction());
        KafkaSource<String> orderCenterSource = KafkaSource.<String>builder().setBootstrapServers(KafkaProperties.KAFKA_BROKER_ADDRESS).setTopics(KafkaProperties.ORDER_SERVER_TOPIC).setGroupId(KafkaProperties.ORDER_SERVER_GROUP_ID)
                .setStartingOffsets(OffsetsInitializer.earliest()).setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("client.id.prefix", "order-center-source") // 添加唯一前缀
                .build();
        SingleOutputStreamOperator<UserClickMsg> orderDataStream = env.fromSource(orderCenterSource, WatermarkStrategy.noWatermarks(), "订单消息")
                .flatMap(new UserMsgFlatMapFunction())
                .map(msg -> {
                    System.out.println("订单数据流处理结果: " + msg);
                    return msg;
                })
                .name("订单数据流");

        // 将多个输入流合并成一个流, 要确保多个流的格式统一
        DataStream<UserClickMsg> unionDataStream = userDataStream.union(orderDataStream);
        unionDataStream.print().name("union流输出结果");

        //做个简单的去重，然后上报到一个指定的topic上，可以做elk日志的记录等能力
        SingleOutputStreamOperator<UserClickMsg> singleOutputStreamOperator =
        unionDataStream.keyBy(
                userClickMsg -> String.join(":",
                String.valueOf(userClickMsg.getUserId()),
                String.valueOf(userClickMsg.getGoodId()),
                String.valueOf(userClickMsg.getAction()),
                userClickMsg.getPlatform())
        ).process(new KeyedProcessFunction<String, UserClickMsg, UserClickMsg>() {
            @Override
            public void processElement(UserClickMsg userClickMsg, KeyedProcessFunction<String, UserClickMsg, UserClickMsg>.Context context, Collector<UserClickMsg> collector) throws Exception {
                collector.collect(userClickMsg);
            }
        }).name("旁路上报kafka消息");

        //所有消息做个旁路，然后上报
        singleOutputStreamOperator.flatMap(new FlatMapFunction<UserClickMsg, String>() {
            @Override
            public void flatMap(UserClickMsg userClickMsg, Collector<String> collector) throws Exception {
                collector.collect(JSON.toJSONString(userClickMsg));
            }
        }).sinkTo(getKafkaSink()).name("去重后上报kafka消息");
//                .addSink(getSinkOutProducer()).name("去重后上报kafka消息");

        //异步处理
//        DataStream<String> asyncStream = AsyncDataStream.unorderedWait(singleOutputStreamOperator, new AsyncHandler(), 5, TimeUnit.SECONDS);
//        asyncStream.addSink(getAsyncProducer()).name("异步流处理结果");
//        asyncStream.print();
        env.execute("test-flink-consumer");
    }

    public static FlinkKafkaProducer<String> getAsyncProducer(){
        Properties rptDataSinkProp = new Properties();
        rptDataSinkProp.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_BROKER_ADDRESS);
        rptDataSinkProp.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 1000 * 700 + "");
        rptDataSinkProp.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        rptDataSinkProp.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        return new FlinkKafkaProducer<String>( "async_handle_result", new SimpleStringSchema(),rptDataSinkProp);
    }

//    public static FlinkKafkaProducer<String> getSinkOutProducer() {
//        Properties rptDataSinkProp = new Properties();
//        rptDataSinkProp.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_BROKER_ADDRESS);
//        rptDataSinkProp.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 1000 * 700 + "");
//        rptDataSinkProp.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
//        rptDataSinkProp.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
//        return new FlinkKafkaProducer<String>( "sing_out_topic", new SimpleStringSchema(),rptDataSinkProp);
//    }

    public static KafkaSink<String> getKafkaSink(){
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_BROKER_ADDRESS);
        kafkaProperties.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "600000");
        kafkaProperties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        kafkaProperties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        // 推荐添加重试配置
        kafkaProperties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        kafkaProperties.setProperty(ProducerConfig.ACKS_CONFIG, "all");

        return KafkaSink.<String>builder().setBootstrapServers(KafkaProperties.KAFKA_BROKER_ADDRESS)
                .setKafkaProducerConfig(kafkaProperties)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("sing_out_topic")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setDeliveryGuarantee(org.apache.flink.connector.base.DeliveryGuarantee.EXACTLY_ONCE)
                .build();
    }


}
