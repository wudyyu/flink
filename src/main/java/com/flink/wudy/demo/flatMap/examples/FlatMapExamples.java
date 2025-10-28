package com.flink.wudy.demo.flatMap.examples;

import com.flink.wudy.demo.flatMap.models.FlatMapInputModel;
import com.flink.wudy.demo.flatMap.models.FlatMapOutputModel;
import com.flink.wudy.demo.flatMap.source.FlatMapDefineSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlatMapExamples {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(ParameterTool.fromArgs(args).getConfiguration());

        DataStream<FlatMapInputModel> source = env.addSource(new FlatMapDefineSource());

        // FlatMapFunction没有返回值
        DataStream<FlatMapOutputModel> transformation =
                source.flatMap(new FlatMapFunction<FlatMapInputModel, FlatMapOutputModel>() {
            @Override
            public void flatMap(FlatMapInputModel flatMapInputModel, Collector<FlatMapOutputModel> collector) throws Exception {
                if ("page2".equals(flatMapInputModel.getLog().getPage())){
                    collector.collect(
                            new FlatMapOutputModel(flatMapInputModel.getUserName(), flatMapInputModel.getLog())

                    );
                }
            }
        });
//                        .keyBy(new KeySelector<FlatMapOutputModel, String>() {
//                    @Override
//                    public String getKey(FlatMapOutputModel flatMapOutputModel) throws Exception {
//                        return flatMapOutputModel.getUserName();
//                    }
//                });
        DataStreamSink<FlatMapOutputModel> sink = transformation.print();

        env.execute();
    }
}
