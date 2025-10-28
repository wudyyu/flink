package com.flink.wudy.window.tumbling.example;

import com.flink.wudy.window.tumbling.model.AggregateInputModel;
import com.flink.wudy.window.tumbling.model.AggregateOutputModel;
import com.flink.wudy.window.tumbling.source.AggregateProductSource;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Date;

/**
 * 【增量窗口处理函数】
 *  大多数场景下优先使用增量处理函数
 *
 * AggregationFunction包含以下4个方法：
 * 1.ACC  createAccumulator(): 在创建一个新窗口时调用，也就是每个窗口的第一条数据到达时调用，该方法会初始化一个用于存储当前窗口中间结果的累加器，并作为方法返回值返回.
 *                              每个新词创建时都会初始化新的累加器，窗口算子会讲累加器存储在状态中
 *
 * 2.ACC add(IN value, ACC accumulator): 在接收到窗口的输入数据时调用，该方法会讲每一条输入数据与旧累加器的结果聚合，得到新累加器的结果。value是输入的数据、 accumulator
 *                                      是当前窗口的累加器，方法的返回值是经过聚合结果计算得到的新累加器的结果
 *
 * 3.OUT getResult(ACC accumulator): 在窗口触发器触发时调用，该方法会使用累加器的值计算并得到窗口算子的输出结果。入参accumulator是累加器
 *
 * 4.ACC merge(ACC a, ACC b):在合并窗口的场景中使用，典型的合并窗口就是会话窗口。注意：滚动窗口和滑动窗口都不是合并窗口，所以无需实现该方法
 *
 * 执行流程： 当窗口收到第一条数据时，会调用createAccumulator()方法为这个窗口初始化一个新的累加器。随着窗口数据不断输入，会为每一条数据调用add()
 *          方法执行聚合计算，并将返回结果存储在状态中。最后在窗口触发器触发计算时，调用getResult方法获取当前窗口的结果并输出
 *
 * 案例：使用AggregateFunction函数计算每种商品1min内的平均销售额
 *
 *
 */
public class TumblingAggregateWindowFunctionExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(ParameterTool.fromArgs(args).getConfiguration());
        env.setParallelism(1);

        DataStreamSource<AggregateInputModel> source = env.addSource(new AggregateProductSource());

        WatermarkStrategy<AggregateInputModel> watermarkStrategy =
                WatermarkStrategy
                        // 周期性的生成水位线 有序流：forMonotonousTimestamps()  , 无序流：forBoundedOutOfOrderness()
                        .<AggregateInputModel>forMonotonousTimestamps()
                        .withTimestampAssigner((e, lastRecordTimestamp) -> e.getTimestamp());


        AggregateFunction<AggregateInputModel, Tuple2<AggregateInputModel, Long>, AggregateOutputModel> myAggFunction = new AggregateFunction<AggregateInputModel, Tuple2<AggregateInputModel, Long>, AggregateOutputModel>() {
            @Override
            public Tuple2<AggregateInputModel, Long> createAccumulator() {
                System.out.println("AggregateFunction的初始化累加器");
                return Tuple2.of(null, 0L);
            }

            @Override
            public Tuple2<AggregateInputModel, Long> add(AggregateInputModel inputModel, Tuple2<AggregateInputModel, Long> acc) {
                if (null == acc.f0){
                    acc.f0 = inputModel;
                    acc.f1 = 1L;
                } else {
                    // 累加销售额
                    acc.f0.setIncome(acc.f0.getIncome() + inputModel.getIncome());
                    // 累加销量
                    acc.f1 += 1L;
                }
                return acc;
            }

            @Override
            public AggregateOutputModel getResult(Tuple2<AggregateInputModel, Long> acc) {
                System.out.println("AggregateFunction获取结果");
                return AggregateOutputModel
                        .builder()
                        .productId(acc.f0.getProductId())
                        .windowStart(DateFormatUtils.format(new Date(acc.f0.getTimestamp()), "yyyy-MM-dd HH:mm:ss"))
                        .avgIncome(acc.f0.getIncome() / acc.f1)
                        .allCnt(acc.f1)
                        .allIncome(acc.f0.getIncome())
                        .build();
            }

            @Override
            public Tuple2<AggregateInputModel, Long> merge(Tuple2<AggregateInputModel, Long> acc1, Tuple2<AggregateInputModel, Long> acc2) {
                // 滚动窗口 无需实现merge方法
                return null;
            }
        };

        DataStream<AggregateOutputModel> transformation = source
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy(i -> i.getProductId())
                .window(TumblingEventTimeWindows.of(Time.seconds(60)))
                .aggregate(myAggFunction);

        DataStreamSink<AggregateOutputModel> sink = transformation.print();
        env.execute("TumblingWindow AggregateFunction");
    }


}
