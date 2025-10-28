package com.flink.wudy.sink.strategy.forward.example;


import com.flink.wudy.sink.strategy.forward.model.InputModel;
import com.flink.wudy.sink.strategy.forward.source.UserDefineSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.StreamFlatMap;
import org.apache.flink.streaming.api.operators.StreamSource;

/**
 * Flink上下游算子的subTask之间会进行数据传输，逻辑数据流 Source->Map->KeyBy/Sum->Sink 的flink作业，当Source和Map之间的算子并行度相同时
 * Source和Map之间的subTask会进行一对一的数据传输，而在Map算子和KeyBy/Sum算子之间，是一对多的数据传输
 *
 * 算子间数据传输的策略:
 * 1.Forward【算子间默认的数据传输策略】: 上游算子在传输数据给下游算子时采用一对一的模式（上游算子的一个SubTask只会将数据传输到下游算子的一个SubTask）
 * 上游算子的SubTask[0] -> 下游算子的SubTask[0], 数据按照顺序下发
 * 只有上下游算子并行度相同且 且 ChainingStrategy为ALWAYS 或 HEAD
 *
 * StreamMap 对应的 ChainingStrategy为 ChainingStrategy.ALWAYS
 * StreamSource 对应的 ChainingStrategy为 ChainingStrategy.f
 * StreamFlatMap 对应的 ChainingStrategy为 ChainingStrategy.ALWAYS
 *
 * 以下两种方式可以让算子间的数据传输策略变为 Forward:
 * 1> 上下游算子间满足forward传输条件，默认就是forward，Flink会将上下游算子合并为算子链
 * 2> 通过DataStream.forward 手动设置 【注意：若上下游算子间不满足forward条件则抛出异常】
 *
 * 如何验证Forward传输策略是一对一？
 * Sink算子的subTask和Source的subTask输出结果下标一致
 */
public class ForwardExamples {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(ParameterTool.fromArgs(args).getConfiguration());

        env.setParallelism(4);

        DataStreamSource<InputModel> source = env.addSource(new UserDefineSource());
        SingleOutputStreamOperator<InputModel> transformation =
        // 1.无需设置forward，满足条件默认就是forward
        source.map(new MapFunction<InputModel, InputModel>() {
            @Override
            public InputModel map(InputModel inputModel) throws Exception {
                return inputModel;
            }
        })
        // 2.主动设置Forward
        .forward()
        .filter(new FilterFunction<InputModel>() {
            @Override
            public boolean filter(InputModel inputModel) throws Exception {
                return true;
            }
        })
        .name("forward source");

       DataStreamSink<InputModel> sink = transformation.print().name("forward sink");
       env.execute("Flink Forward Strategy");
    }

}
