package com.flink.wudy.window.sliding.example;

import com.flink.wudy.window.sliding.model.InputModel;
import com.flink.wudy.window.sliding.model.OutputModel;
import com.flink.wudy.window.sliding.source.UserSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * 滑动窗口: 统计电商网站的同时在线用户数(一个用户的心跳日志每1min 上报一次)
 *
 */
public class SlidingWindowExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(ParameterTool.fromArgs(args).getConfiguration());
        // TODO:通过参数传入
        env.setParallelism(1);
        DataStreamSource<InputModel> source = env.addSource(new UserSource());
        WatermarkStrategy<InputModel> watermarkStrategy = WatermarkStrategy
                // 周期性的生成水位线 有序流：forMonotonousTimestamps()  , 无序流：forBoundedOutOfOrderness()
                .<InputModel>forMonotonousTimestamps()
                // 从数据中获取时间戳作为事件时间语义下的时间戳
                .withTimestampAssigner((e, lastRecordTimeStamp) -> e.getTimestamp());

        SingleOutputStreamOperator<OutputModel> transformation = source.assignTimestampsAndWatermarks(watermarkStrategy)
                // 注意：使用windowAll存在数据倾斜风险
                // 设置窗口大小2min, 滑动步长1min 的滑动窗口
                .windowAll(SlidingEventTimeWindows.of(Time.minutes(2), Time.minutes(1)))
                // 使用窗口处理函数计算在线用户数
                .apply(new AllWindowFunction<InputModel, OutputModel, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<InputModel> input, Collector<OutputModel> out) throws Exception {
                        // 将用户id放入Set中去重
                        Set<Integer> userIds = Lists.newArrayList(input).stream().map(InputModel::getUserId).collect(Collectors.toSet());
                        out.collect(OutputModel
                                .builder()
                                .uv(userIds.size())
                                .minuteStartTimestamp(timeWindow.getStart())
                                .build()
                        );
                    }
                });
        DataStreamSink<OutputModel> sink = transformation.print();
        env.execute("Sliding Sink");

    }
}
