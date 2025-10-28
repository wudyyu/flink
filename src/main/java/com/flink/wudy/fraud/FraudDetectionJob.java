package com.flink.wudy.fraud;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.source.TransactionSource;
import org.apache.flink.walkthrough.common.sink.AlertSink;
import org.apache.flink.walkthrough.common.entity.Alert;

public class FraudDetectionJob {
    public static void main(String[] args) throws Exception {
        // 1.任务执行环境用于定义任务的属性、创建数据源以及最终启动任务的执行
        StreamExecutionEnvironment streamExecutionEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.创建数据源
        /**
         * 数据源从外部系统例如 Apache Kafka、Rabbit MQ 或者 Apache Pulsar 接收数据，然后将数据送到 Flink 程序中
         */
        DataStream<Transaction> transactions  = streamExecutionEnv.addSource(new TransactionSource()).name("transactions");

        // 3.对事件分区 & 欺诈检测
        /**
         * transactions 这个数据流包含了大量的用户交易数据，需要被划分到多个并发上进行欺诈检测处理
         *
         */
        DataStream<Alert> alerts = transactions.keyBy(Transaction::getAccountId).process(new FraudDetector()).name("fraud-detector");

        // 4.输出结果
        /**
         * sink 会将 DataStream 写出到外部系统，例如 Apache Kafka、Cassandra 或者 AWS Kinesis 等。
         * AlertSink 使用 INFO 的日志级别打印每一个 Alert 的数据记录，而不是将其写入持久存储
         */
        alerts.addSink(new AlertSink()).name("send-alerts");

        // 5.运行作业
        /**
         * Flink 程序是懒加载的，并且只有在完全搭建好之后，才能够发布到集群上执行。
         * 调用 StreamExecutionEnvironment#execute 时给任务传递一个任务名参数，就可以开始运行任务
         */
        streamExecutionEnv.execute("Fraud Detection");

    }
}
