package com.flink.wudy.window.session.example;

import com.flink.wudy.window.session.model.InputModel;
import com.flink.wudy.window.session.model.OutputModel;
import com.flink.wudy.window.session.source.UserSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 会话窗口:统计用户使用电商APP时浏览商品的次数，超过5分钟没有浏览行为就认为用户下线了
 */
public class SessionWindowExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(ParameterTool.fromArgs(args).getConfiguration());
        // TODO:通过参数传入
        env.setParallelism(1);

        DataStreamSource<InputModel> source = env.addSource(new UserSource());
        WatermarkStrategy<InputModel> watermarkStrategy = WatermarkStrategy
                // 周期性的生成水位线 有序流：forMonotonousTimestamps()  , 无序流：forBoundedOutOfOrderness()
                .<InputModel>forMonotonousTimestamps()
                // 从数据中获取时间戳作为事件时间语义下的时间戳
                .withTimestampAssigner((e, lastRecordTimestamp) -> e.getTimestamp());

        SingleOutputStreamOperator<OutputModel> transformation = source
                // 事件时间需要分配Watermark
                .assignTimestampsAndWatermarks(watermarkStrategy)
                // 按照用户分组
                .keyBy(i -> i.getUserId())
                // 会话间隔为5分钟的会话窗口
                .window(EventTimeSessionWindows.withGap(Time.minutes(5)))
                .apply(new WindowFunction<InputModel, OutputModel, Integer, TimeWindow>() {
                    @Override
                    public void apply(Integer userId, TimeWindow timeWindow, Iterable<InputModel> input, Collector<OutputModel> out) throws Exception {
                        System.out.println("进入apply," + "开始时间=" + timeWindow.getStart() + "结束时间=" + timeWindow.getEnd());
                        out.collect(
                                OutputModel
                                        .builder()
                                        .userId(userId)
                                        // 窗口中数据集合的大小就是浏览商品次数
                                        .count(Lists.newArrayList(input).size())
                                        .sessionStartTimestamp(timeWindow.getStart())
                                        .build()
                        );
                    }
                });
        DataStreamSink<OutputModel> sink = transformation.print();
        env.execute("Session Window");
    }
}
