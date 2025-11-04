package com.flink.wudy.sink.kafka.example;

import com.flink.wudy.sink.kafka.model.CustomInputModel;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.springframework.util.ObjectUtils;

import java.util.Properties;

/*
* 从kafka消费数据(Source) 并 写入到kafka(Sink)
* */
public class KafkaSourceSinkExample {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(ParameterTool.fromArgs(args).getConfiguration());
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("group.id", "kafka-source-sink-example");
//        SourceFunction<String> sourceFunction = new FlinkKafkaConsumer<String>(
//                "flink-sink-topic3",
//                new SimpleStringSchema(),
//                properties
//        );

        FlinkKafkaConsumer<String> sourceConsumer = new FlinkKafkaConsumer<String>(
                "flink-sink-topic3",
                new SimpleStringSchema(),
                properties
        );
        sourceConsumer.setStartFromEarliest();
                
        // 从名为flink-sink-topic3的Topic中读取数据
        DataStreamSource<String> sourceStream = env.addSource(sourceConsumer);
        SingleOutputStreamOperator<String> sinkStream = sourceStream.map(new JsonToModelMapper()).map(new CountIncrementMapper()).map(new ModelToJsonMapper());

        // 重新写入到新的Topic
        FlinkKafkaProducer<String> sinkProducer = new FlinkKafkaProducer<String>(
            "flink-sink-topic4",
            new SimpleStringSchema(),
            properties
        );
        sinkStream.addSink(sinkProducer);

        // 检查配置点
        env.enableCheckpointing(5000);

        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // 打印处理前后的数据用于调试
        sourceStream.print();
        sinkStream.print();

        // 执行
        env.execute("Flink Kafka Data Processor");
    }

    public static class ModelToJsonMapper implements MapFunction<CustomInputModel, String> {

        @Override
        public String map(CustomInputModel customInputModel) throws Exception {
            try {
                return objectMapper.writeValueAsString(customInputModel);
            }catch (JsonProcessingException e){
                System.out.println("json 字符串序列化失败");
            }
            return "{}";
        }
    }

    public static class CountIncrementMapper implements MapFunction<CustomInputModel, CustomInputModel> {
        @Override
        public CustomInputModel map(CustomInputModel customInputModel) throws Exception {
            if (!ObjectUtils.isEmpty(customInputModel.getCount())){
                // 对原商品销量 +100
                customInputModel.setCount(customInputModel.getCount() + 100);
            }
            return customInputModel;
        }
    }

    public static class JsonToModelMapper implements MapFunction<String, CustomInputModel> {

        @Override
        public CustomInputModel map(String s) throws Exception {
            try {
                return objectMapper.readValue(s, CustomInputModel.class);
            }catch (Exception e){
                System.out.println("json 字符串解析失败");
            }
            return new CustomInputModel();
        }
    }
}


