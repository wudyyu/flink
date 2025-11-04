package com.flink.wudy.sink.kafka.example;

import com.flink.wudy.sink.kafka.model.CustomInputModel;
import com.flink.wudy.sink.kafka.source.CustomSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * 1> Kafka Topic作为数据源存储引擎时，需要给FlinkKafkaConsumer传入DeserializationSchema来将Kafka中的二进制数据反序列化为Java对象
 *
 * 2> Kafka Topic作为数据汇存储引擎时，需要给FlinkKafkaProducer传入SerializationSchema将Flink中的Java对象 序列化为 Kafka Topic能够存储的二进制数据
 *
 */
public class kafkaProducerExample {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty("group.id", "flink-kafka-group");
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "3600000");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(ParameterTool.fromArgs(args).getConfiguration());
        env.setParallelism(2);
        env.setMaxParallelism(6);

        // 添加数据源
        DataStreamSource<CustomInputModel> source = env.addSource(new CustomSource());

        SingleOutputStreamOperator<CustomInputModel> transformation =
        // transformation 处理逻辑
        source.keyBy(new KeySelector<CustomInputModel, String>() {
            @Override
            public String getKey(CustomInputModel customInputModel) throws Exception {
                return customInputModel.getProductId();
            }
        }).reduce(new ReduceFunction<CustomInputModel>() {
            @Override
            public CustomInputModel reduce(CustomInputModel acc, CustomInputModel in) throws Exception {
                acc.setIncome(acc.getIncome() + in.getIncome());
                acc.setCount(acc.getCount() + in.getCount());
                acc.setAvgPrice(acc.getAvgPrice().add(in.getAvgPrice()));
                return acc;
            }
        });

        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<String>(
                "flink-sink-topic3", new SimpleStringSchema(), properties
        );

//        FlinkKafkaProducer011 producer = new FlinkKafkaProducer011("flink-sink-topic", new SimpleStringSchema(), properties,FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);

        // 写入sink
        transformation.map(new MapFunction<CustomInputModel, String>() {
            // 将对象转为字符串
            @Override
            public String map(CustomInputModel customInputModel) throws Exception {
//                return customInputModel.toString();
                return objectMapper.writeValueAsString(customInputModel);
            }
        }).addSink(producer).name("kafka transformation");

        env.execute("kafka producer task");
    }
}
