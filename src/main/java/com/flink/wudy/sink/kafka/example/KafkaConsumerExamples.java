package com.flink.wudy.sink.kafka.example;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;


/**
 * 前置操作: 需要先安装kafka
 * 1.Mac下Docker快速安装:
 * >拉取镜像: docker pull lensesio/fast-data-dev  [https://github.com/lensesio/fast-data-dev]
 * >启动镜像：docker run --rm -it     -p 2181:2181 -p 3030:3030 -p 8091:8091     -p 8092:8092 -p 8093:8093 -p 9092:9092 -e ADV_HOST=127.0.0.1 lensesio/fast-data-dev
 * >打开网页，进入: http://127.0.0.1:3030
 * >镜像的terminal不能关掉，重新新开一个teminal:docker run --rm -it --net=host lensesio/fast-data-dev bash
 * >创建topic: kafka-topics --create --topic flink-topic --partitions 3 --replication-factor 1 --bootstrap-server 127.0.0.1:9092
 * >查看topic:
 *  docker run --rm -it --net=host lensesio/fast-data-dev bash [进入镜像命令行模式]
 *  kafka-topics --list --bootstrap-server localhost:9092
 *
 * >删除topic
 *  kafka-topics  --bootstrap-server localhost:9092 --delete --topic   flink-sink-topic4
 *
 * >producer写入数据: kafka-console-producer --broker-list 127.0.0.1:9092 --topic flink-topic
 * >consumer消费数据：kafka-console-consumer  --bootstrap-server localhost:9092 --topic flink-sink-topic --from-beginning
 *
 *
 * 数据汇(Kafka)
 * 从数据源kafka topic读取数据，将处理完的结果输出到外部数据存储引擎
 * 相关API：
 * > DataStream.addSink(SinkFunction) 将DataStream的数据写到外部数据存储引擎中, SinkFunction用于定义在如何链接数据汇存储引擎以及如何将数据写到DataStream
 * > SinkFunction和SourceFunction一样，在Flink中都是用于连接外部数据存储引擎的模块，被称作Connector，SinkFunction 也称作Sink Connector
 * > Apache官方预置了以下Sink Connector：
 *   Apache Kafka (source/sink)
 *   Apache Cassandra (sink)
 *   Amazon Kinesis Streams (source/sink)
 *   ElasticSearch (sink)
 *   FileSystem (sink)
 *   RabbitMQ (source/sink)
 *   Google Pubsub (source/sink)
 *   Apache Pulsar (source)
 *   JDBC (sink)
 *
 */
public class KafkaConsumerExamples {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(ParameterTool.fromArgs(args).getConfiguration());

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("group.id", "flink-kafka-group");
        FlinkKafkaConsumer<String> sourceFunction =
        new FlinkKafkaConsumer<String>(
                "flink-sink-topic3",
                // Flink将数据写入到Kafka Topic时，将数据序列化作为二进制数据时的序列化器，SimpleStringSchema可以将String反序列化为byte[]
                new SimpleStringSchema(),
                properties
        );
        // 指定偏移量
        sourceFunction.setStartFromLatest();

        // 从 flink-topic 的Kafka topic中读取数据
       DataStreamSource<String> source = env.addSource(sourceFunction);
        // 开启checkpoint 每5000ms 一次
        env.enableCheckpointing(5000);
        // 设置有且仅有一次模式 目前支持 EXACTLY_ONCE/AT_LEAST_ONCE
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 设置checkpoint的存储位置
//        env.getCheckpointConfig().setCheckpointStorage("hdfs:///flink/checkpoints");

        source.print();
        env.execute("Flink Kafka Consumer Example");
    }
}
