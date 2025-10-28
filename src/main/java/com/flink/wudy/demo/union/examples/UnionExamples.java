package com.flink.wudy.demo.union.examples;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Union多条流合并为一条流，但要求多条流的数据类型相同
 * 合并为一条流后， 多条流之间数据无序，但来自同一条流的数据有序
 */
public class UnionExamples {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Integer> dataStream1 = env.fromElements(111,222,333);
        DataStream<Integer> dataStream2 = env.fromElements(444,555,666);

        // 应用union操作
        DataStream<Integer> transformation = dataStream1.union(dataStream2);
        DataStreamSink<Integer> sink = transformation.print();

        // 打印结果
//        dataStream3.map(new MapFunction<Integer, String>() {
//            @Override
//            public String map(Integer value) throws Exception {
//                return value.toString();
//            }
//        }).print();

        env.execute("Flink Union Example");
    }
}
