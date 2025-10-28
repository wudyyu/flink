package com.flink.wudy.window.tumbling.example;

import com.flink.wudy.window.tumbling.model.ProcessInputModel;
import com.flink.wudy.window.tumbling.model.ProcessOutputModel;
import com.flink.wudy.window.tumbling.source.ProcessProductSource;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.IterableUtils;

import java.util.Date;

/**
 * 【全量窗口处理函数】
 * 推荐：在生产环境使用ProcessWidowFunction
 *
 * 窗口处理函数：
 * 1> 全量窗口处理函数
 * 2> 增量窗口处理函数
 * 3> 增量、全量搭配使用
 *
 * ProcessWindowFunction提供了和WindowFunction类似的功能，区别在于ProcessWindowFunction可以获取运行时上下文，提供了以下功能:
 * 1.访问窗口信息：通过Context的window()方法获取当前时间窗口的开始时间和结束时间
 * 2.访问时间信息:通过Context的currentProcessingTime()方法和currentWatermark()方法分别获取当前subTask的处理时间和事件时间的watermark
 * 3.访问状态：通过Context的windowState()方法访问当前key下窗口内的状态，通过Context的globalState()方法可以访问当前key的状态，这里访问的状态时跨窗口的
 * 4.旁路输出：通过Context的output(OutputTag<X> outputTag, X value) 方法可以将数据输出到指定旁路中
 *
 * 全量窗口处理函数: 状态大、执行效率低。
 * > 状态大: 假如1min内所有用户浏览记录10万条，在窗口触发计算前，窗口算子会在状态中缓存这10万条原始记录而不进行计算，导致窗口算子的存储压力比较大
 * > 执行效率低: 只有在到达窗口结束时间，触发窗口触发器，窗口处理函数才能获取这10万条原始数据进行计算，因此Flink作业的资源使用率呈现时而空载、时而满载的情况
 *
 * 应用场景：
 * > 全量窗口处理函数的特点是在窗口触发时获取全量数据，在同步IO低效问题解决方案中批量访问外部接口，先通过全量窗口拿到1万条数据，再切分为1000条进行外部接口请求
 *
 * 案例：统计每件商品过去1min内的访问次数(pv)以及截止当前这1min的历史累计访问次数【全量窗口处理函数案例】
 */
public class TumblingProcessWindowFunctionExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(ParameterTool.fromArgs(args).getConfiguration());
        env.setParallelism(1);
        DataStreamSource<ProcessInputModel> source = env.addSource(new ProcessProductSource());

        WatermarkStrategy<ProcessInputModel> watermarkStrategy = WatermarkStrategy
                // 周期性的生成水位线 有序流：forMonotonousTimestamps()  , 无序流：forBoundedOutOfOrderness()
                .<ProcessInputModel>forMonotonousTimestamps()
                // 从数据中获取时间戳作为事件时间语义下的时间戳
                .withTimestampAssigner((e, lastRecordTimestamp) -> e.getTimestamp());

        SingleOutputStreamOperator<ProcessOutputModel> transformation =
        source
                // 生成水位线
                .assignTimestampsAndWatermarks(watermarkStrategy)
                // 按照商品类型分类
                .keyBy(i -> i.getProductId())
                // 1min的滚动时间窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(60)))
                .process(new ProcessWindowFunction<ProcessInputModel, ProcessOutputModel, String, TimeWindow>() {
                    // 使用ValueState(状态)来存储历史累计访问次数
                    private ValueStateDescriptor<Integer> cumulatePvValueStateDescriptor = new ValueStateDescriptor<Integer>("cumulate-pv", Integer.class);

                    @Override
                    public void process(String productId, ProcessWindowFunction<ProcessInputModel, ProcessOutputModel, String, TimeWindow>.Context context, Iterable<ProcessInputModel> input, Collector<ProcessOutputModel> out) throws Exception {
                        // 集合的大小就是1min的访问次数
                        Integer minutePv = Math.toIntExact(IterableUtils.toStream(input).count());

                        String windowStart = DateFormatUtils.format(new Date(context.window().getStart()), "yyyy-MM-dd HH:mm:ss");

                        // 输出这1min内的访问次数
                        out.collect(
                                ProcessOutputModel
                                        .builder()
                                        .pv(minutePv)
                                        .windowStart(windowStart)
                                        .productId(productId)
                                        .type("1min内访问次数")
                                        .build()
                        );

                        // 通过上下文的globalState()方法获取用于存储历史累计的访问次数
                        ValueState<Integer> cumulatePvValueState = context.globalState().getState(cumulatePvValueStateDescriptor);
                        Integer cumulatePv = cumulatePvValueState.value();
                        // 为null时表示时第一次访问，默认设置为0
                        cumulatePv = (null == cumulatePv) ? 0 : cumulatePv;
                        // 历史累计访问次数 = 旧历史累计访问次数 + 当前1min的访问次数
                        cumulatePv += minutePv;

                        // 将历史累计访问次数的结果存储到状态中
                        cumulatePvValueState.update(cumulatePv);
                        // 输出历史累计访问次数的结果
                        out.collect(
                                ProcessOutputModel
                                        .builder()
                                        .productId(productId)
                                        .windowStart(windowStart)
                                        .pv(cumulatePv)
                                        .type("历史累计访问次数")
                                        .build()
                        );
                    }
                });
        DataStreamSink<ProcessOutputModel> sink = transformation.print();
        env.execute("Tumbling ProcessWindowFunction");
    }
}
