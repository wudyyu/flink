package com.flink.wudy.demo.connect.example;

import com.flink.wudy.demo.connect.models.ConnectInputModel1;
import com.flink.wudy.demo.connect.models.ConnectInputModel2;
import com.flink.wudy.demo.connect.models.OutputModel;
import com.flink.wudy.demo.connect.source.ConnectSource1;
import com.flink.wudy.demo.connect.source.ConnectSource2;
import com.flink.wudy.demo.flatMap.models.LogModel;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * Connect将多条流(DataStream)合并为一条(ConnectedStreams)
 * 常使用CoMap、CoFlatMap将两条不同类型的输入数据流转换为同一种类型的输出数据流
 */
public class ConnectExamples {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(ParameterTool.fromArgs(args).getConfiguration());
        DataStreamSource<ConnectInputModel1> source1 = env.addSource(new ConnectSource1());
        DataStreamSource<ConnectInputModel2> source2 = env.addSource(new ConnectSource2());

        ConnectedStreams<ConnectInputModel1, ConnectInputModel2> connectedStreams = source1.connect(source2);

        DataStream<OutputModel> transformation =
        connectedStreams.flatMap(new CoFlatMapFunction<ConnectInputModel1, ConnectInputModel2, OutputModel>() {
            // source1处理处理
            @Override
            public void flatMap1(ConnectInputModel1 value, Collector<OutputModel> out) throws Exception {
                out.collect(
                        new OutputModel(value.getUserName(), value.getLog())
                );
            }

            // source2处理处理
            @Override
            public void flatMap2(ConnectInputModel2 value, Collector<OutputModel> out) throws Exception {
                for (LogModel log : value.getLogs()){
                    out.collect(
                            new OutputModel(value.getUserName(), log)
                    );
                }
            }
        });

        DataStreamSink<OutputModel> sink = transformation.print();
        env.execute("Flink Connected Example");
    }
}
