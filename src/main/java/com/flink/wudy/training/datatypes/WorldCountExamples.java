package com.flink.wudy.training.datatypes;

import com.flink.wudy.training.sources.UserDefineSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class WorldCountExamples {

    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 使用带有Flink Web UI的上下文环境(访问地址：http://localhost:8081)
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(ParameterTool.fromArgs(args).getConfiguration());

        //设置算子并行度
        env.setParallelism(1);

        //从自定义数据源中读取数据
        DataStreamSource<String> source = env.addSource(new UserDefineSource());
        DataStream<Tuple2<String, Integer>> singleWorldDataStream =
        source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                Arrays.asList(s.split(" "))
                        .forEach(singleWorld -> collector.collect(Tuple2.of(singleWorld, 1)));
            }
        });

        DataStream<Tuple2<String, Integer>> wordCountDataStream = singleWorldDataStream.keyBy(v -> v.f0).sum(1);
        DataStreamSink<Tuple2<String, Integer>> sink = wordCountDataStream.print();
        env.execute();
    }
}
