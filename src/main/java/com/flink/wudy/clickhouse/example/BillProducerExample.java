package com.flink.wudy.clickhouse.example;

import com.alibaba.fastjson.JSON;
import com.flink.wudy.clickhouse.model.BillEntityModel;
import com.flink.wudy.clickhouse.source.BillEntitySource;
import com.flink.wudy.config.KafkaProperties;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.math.BigDecimal;
import java.util.Properties;
import java.util.UUID;

/**
 * 将数据源的数据处理后写入到kafka
 */
public class BillProducerExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(ParameterTool.fromArgs(args).getConfiguration());
        env.setParallelism(2);
        env.setMaxParallelism(6);

        DataStreamSource<BillEntityModel> source = env.addSource(new BillEntitySource());

        SingleOutputStreamOperator<BillEntityModel> transformation =
        source.keyBy(new KeySelector<BillEntityModel, String>() {
            @Override
            public String getKey(BillEntityModel billEntityModel) throws Exception {
                return String.join("-", String.valueOf(billEntityModel.getProjectId()), billEntityModel.getRegionId(), billEntityModel.getItemId());
            }
        }).reduce(new ReduceFunction<BillEntityModel>() {
            @Override
            public BillEntityModel reduce(BillEntityModel acc, BillEntityModel in) throws Exception {
                acc.setBillId(UUID.randomUUID().toString());
                acc.setPrice(acc.getPrice().add(in.getPrice()));
                Long min = Math.min(acc.getStartTime(), in.getStartTime());
                acc.setStartTime(min);
                Long max = Math.max(acc.getEndTime(), in.getEndTime());
                acc.setEndTime(max);
                return acc;
            }
        });

        transformation.map(new MapFunction<BillEntityModel, String>() {
            @Override
            public String map(BillEntityModel billEntityModel) throws Exception {
                return JSON.toJSONString(billEntityModel);
            }
        }).sinkTo(getKafkaProducer()).name("写入kafka");

        env.execute("bill producer task");
    }

    public static KafkaSink<String> getKafkaProducer(){
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_BROKER_ADDRESS);
        kafkaProperties.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "600000");
        kafkaProperties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        kafkaProperties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        // 推荐添加重试配置
        kafkaProperties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        kafkaProperties.setProperty(ProducerConfig.ACKS_CONFIG, "all");

        return KafkaSink.<String>builder()
                .setBootstrapServers(KafkaProperties.KAFKA_BROKER_ADDRESS)
                .setKafkaProducerConfig(kafkaProperties)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("bill_post_paid_topic")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(org.apache.flink.connector.base.DeliveryGuarantee.EXACTLY_ONCE)
                // 配置 DeliveryGuarantee.EXACTLY_ONCE 时，必须设置事务 ID 前缀
                .setTransactionalIdPrefix("bill-")
                .build();
    }
}
