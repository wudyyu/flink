package com.flink.wudy.clickhouse.example;

import com.flink.wudy.clickhouse.model.BillEntityModel;
import com.flink.wudy.clickhouse.transfer.BillFlatMapFunction;
import com.flink.wudy.clickhouse.transfer.ClickHouseSinkFunction;
import com.flink.wudy.config.KafkaProperties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 1.读取到kafka中处理后的数据，然后写入ck中
   2.CK表结构
     CREATE TABLE wudy.bill
     (
     `bill_id` String,
     `project_id` Int64,
     `price` Decimal(18, 2),
     `region_id` String,
     `item_id` String,
     `charge_type` String,
     `created_at` DateTime,
     `start_time` UInt64,
     `end_time` UInt64
     )
     ENGINE = ReplacingMergeTree
     PARTITION BY toYYYYMM(created_at)
     ORDER BY (created_at, bill_id, project_id, region_id, item_id)
     SETTINGS index_granularity = 8192;

 */
public class BillConsumerExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(ParameterTool.fromArgs(args).getConfiguration());
        KafkaSource<String> billSource = getKafkaSource();
        SingleOutputStreamOperator<BillEntityModel> billDataStream = env.fromSource(billSource, WatermarkStrategy.noWatermarks(), "账单消息").flatMap(new BillFlatMapFunction()).name("账单流处理");

        billDataStream.addSink(new ClickHouseSinkFunction()).name("clickhouse写入").setParallelism(2);

        env.execute("Bill Data to ClickHouse");
    }

    private static KafkaSource<String> getKafkaSource(){
        return KafkaSource.<String>builder()
                .setBootstrapServers(KafkaProperties.KAFKA_BROKER_ADDRESS)
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setTopics("bill_post_paid_topic")
                .setGroupId("bill_post_paid")
                .setProperty("client.id.prefix", "bill-post-paid-source")
                .build();
    }
}
