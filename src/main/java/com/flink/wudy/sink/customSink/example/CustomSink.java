package com.flink.wudy.sink.customSink.example;

import com.flink.wudy.sink.customSink.sink.UserDefineSink;
import com.flink.wudy.sink.customSink.source.UserDefineSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 自定义数据汇
 * > 当Flink预置的Sink Connector无法满足需求时，可以通过Sink-Function接口(或者继承RickSinkFunction抽象类)将数据写入数据汇存储引擎
 * > SinkFunction的invoke方法定义了一个将结果数据打印到日志中的SinkFunction
 * > invoke的调用时机：Flink作业在运行时，Sink算子每收到上游算子下发的一条数据，就会调用一次invoke方法
 */
public class CustomSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(ParameterTool.fromArgs(args).getConfiguration());
        DataStreamSource<String> source = env.addSource(new UserDefineSource());
        SingleOutputStreamOperator<String> transformation = source.map(v -> v.split(" ")[0] + "-user-custom-define-sink");
        DataStreamSink<String> sink = transformation.addSink(new UserDefineSink()).name("customSink");
        env.execute("Flink Custom Sink Example");
    }
}
