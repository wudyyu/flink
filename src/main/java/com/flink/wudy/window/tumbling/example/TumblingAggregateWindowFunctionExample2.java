package com.flink.wudy.window.tumbling.example;

import com.flink.wudy.window.tumbling.model.AggregateInputModel;
import com.flink.wudy.window.tumbling.model.AggregateOutputModel;
import com.flink.wudy.window.tumbling.source.AggregateProductSource;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;

/**
 * 【全量+增量窗口处理函数】
 *  增量窗口缺点： 不能获取作业运行时的上下文信息
 *  解决办法：借助全量窗口处理函数的ProcessWindowFunction
 *
 * 案例：使用AggregateFunction函数计算每种商品1min内的平均销售额
 */
public class TumblingAggregateWindowFunctionExample2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(ParameterTool.fromArgs(args).getConfiguration());
        env.setParallelism(1);

        DataStreamSource<AggregateInputModel> source = env.addSource(new AggregateProductSource());

        WatermarkStrategy<AggregateInputModel> watermarkStrategy =
                WatermarkStrategy
                        // 周期性的生成水位线 有序流：forMonotonousTimestamps()  , 无序流：forBoundedOutOfOrderness()
                        .<AggregateInputModel>forMonotonousTimestamps()
                        .withTimestampAssigner((e, lastRecordTimestamp) -> e.getTimestamp());

        AggregateFunction<AggregateInputModel, Tuple2<AggregateInputModel, Long>, AggregateOutputModel> myAggFunction =
        new AggregateFunction<AggregateInputModel, Tuple2<AggregateInputModel, Long>, AggregateOutputModel>(){
            @Override
            public Tuple2<AggregateInputModel, Long> createAccumulator() {
                System.out.println("AggregateFunction的初始化累加器");
                return Tuple2.of(null, 1L);
            }

            @Override
            public Tuple2<AggregateInputModel, Long> add(AggregateInputModel inputModel, Tuple2<AggregateInputModel, Long> acc) {
                if (null == acc){
                    acc.f0 = inputModel;
                    acc.f1 = 1L;
                } else {
                    acc.f0.setIncome(acc.f0.getIncome() + inputModel.getIncome());
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
                        .allIncome(acc.f0.getIncome())
                        .allCnt(acc.f1)
                        .avgIncome(acc.f0.getIncome() / acc.f1)
                        .build();
            }

            @Override
            public Tuple2<AggregateInputModel, Long> merge(Tuple2<AggregateInputModel, Long> aggregateInputModelLongTuple2, Tuple2<AggregateInputModel, Long> acc1) {
                return null;
            }
        };

        DataStream<AggregateOutputModel> transformation =
        source
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy(i -> i.getProductId())
                .window(TumblingEventTimeWindows.of(Time.seconds(60)))
                .aggregate(myAggFunction, new ProcessWindowFunction<AggregateOutputModel, AggregateOutputModel, String, TimeWindow>() {
                    // 使用ValueState存储每种商品的历史累计销售额和销量
                    private ValueStateDescriptor<Tuple2<Long, Long>> historyInfoValueStateDescriptor = new ValueStateDescriptor<Tuple2<Long, Long>>("history-info", TypeInformation.of(
                            new TypeHint<Tuple2<Long, Long>>() {
                            }));

                    // Iterable<AggregateOutputModel> iterable 中只包含一条数据
                    @Override
                    public void process(String s, ProcessWindowFunction<AggregateOutputModel, AggregateOutputModel, String, TimeWindow>.Context context, Iterable<AggregateOutputModel> iterable, Collector<AggregateOutputModel> out) throws Exception {
                        long windowStart = context.window().getStart();
                        // 通过globalState获取历史累计销售额和销量
                        ValueState<Tuple2<Long, Long>> historyInfoValueState = context.globalState().getState(historyInfoValueStateDescriptor);
                        for (AggregateOutputModel e : iterable){
                            System.out.println("ProcessWindowFunction 输出1min内的增量数据");
                            e.setTimeStamp(windowStart);
                            // 输出当前1Min内的销售额、销量、平均销售额
                            out.collect(e);
                            Tuple2<Long, Long> historyInfoValue = historyInfoValueState.value();
                            if (null == historyInfoValue){
                                historyInfoValue = Tuple2.of(e.getAllCnt(), e.getAllIncome());
                            } else {
                                historyInfoValue = Tuple2.of(e.getAllCnt() + historyInfoValue.f0, e.getAllIncome() + historyInfoValue.f1);
                            }
                            // 使用historyInfoValueState 存储累计销售额和销量
                            historyInfoValueState.update(historyInfoValue);
                            System.out.println("ProcessWindowFunction 输出历史累计数据");

                            out.collect(
                                    AggregateOutputModel
                                            .builder()
                                            .productId(e.getProductId())
                                            .windowStart(DateFormatUtils.format(new Date(windowStart), "yyyy-MM-dd HH:mm:ss"))
                                            .avgIncome(historyInfoValue.f1 / historyInfoValue.f0)
                                            .allIncome(historyInfoValue.f0)
                                            .allCnt(historyInfoValue.f1)
                                            .build()
                            );
                        }
                    }
                });

        DataStreamSink<AggregateOutputModel> sink = transformation.print();
        env.execute("TumblingWindow AggregateFunction");
    }
}
