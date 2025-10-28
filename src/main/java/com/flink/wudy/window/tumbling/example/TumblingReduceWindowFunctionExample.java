package com.flink.wudy.window.tumbling.example;

import com.flink.wudy.window.tumbling.model.ReduceInputModel;
import com.flink.wudy.window.tumbling.model.ReduceOutputModel;
import com.flink.wudy.window.tumbling.source.ReduceProductSource;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Date;

/**
 * 【增量窗口处理函数】
 *
 * Flink提供了以下两种增量窗口处理函数:
 * 1.ReduceFunction: 用于对数据进行归约聚合计算，要求输入数据、聚合数据、输出数据的类型一致
 * 2.AggregateFunction:
 *  > IN: 输入数据类型
 *  > ACC: 增量计算的中间聚合结果的数据类型
 *  > OUT： 输出数据类型
 *
 *
 *  案例：使用ReduceFunction函数计算每种商品1min内的平均销售额
 */
public class TumblingReduceWindowFunctionExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(ParameterTool.fromArgs(args).getConfiguration());
        env.setParallelism(1);

        DataStreamSource<ReduceInputModel> source = env.addSource(new ReduceProductSource());

        WatermarkStrategy<ReduceInputModel> watermarkStrategy =
        WatermarkStrategy
                // 周期性的生成水位线 有序流：forMonotonousTimestamps()  , 无序流：forBoundedOutOfOrderness()
                .<ReduceInputModel>forMonotonousTimestamps()
                .withTimestampAssigner((e, lastRecordTimestamp) -> e.getTimestamp());

        SingleOutputStreamOperator<ReduceOutputModel> transformation =
        source
                // 生成水位线
                .assignTimestampsAndWatermarks(watermarkStrategy)
                // f1原始数据 ReduceInputModel   f2表示销量 这里默认给的1
                .map(new MapFunction<ReduceInputModel, Tuple2<ReduceInputModel, Long>>() {
                    @Override
                    public Tuple2<ReduceInputModel, Long> map(ReduceInputModel reduceInputModel) throws Exception {
                        return Tuple2.of(reduceInputModel, 1L);
                    }
                })
                .keyBy(i -> i.f0.getProductId())
                .window(TumblingEventTimeWindows.of(Time.seconds(60)))
                // 使用ReduceFunction计算1min内该商品的销售额和销量
                .reduce(new ReduceFunction<Tuple2<ReduceInputModel, Long>>() {
                    @Override
                    public Tuple2<ReduceInputModel, Long> reduce(Tuple2<ReduceInputModel, Long> v1, Tuple2<ReduceInputModel, Long> v2) throws Exception {
                        v1.f0.setIncome(v1.f0.getIncome() + v2.f0.getIncome());
                        v1.f1 += v2.f1;
                        return v1;
                    }
                })
                // 总销售额 除以  总销量 得到1min内的平均销售额
                .map(i -> ReduceOutputModel
                        .builder()
                        .productId(i.f0.getProductId())
                        .windowStart(DateFormatUtils.format(new Date(i.f0.getTimestamp()), "yyyy-MM-dd HH:mm:ss"))
                        .avgIncome(i.f0.getIncome() / i.f1)
                        .build()
                );

        DataStreamSink<ReduceOutputModel> sink = transformation.print();
        env.execute("TumblingWindow ReduceFunction");
    }
}
