package com.flink.wudy.window.tumbling.example;

import com.flink.wudy.io.async.source.UserDefinedSource;
import com.flink.wudy.sink.kafka.model.CustomInputModel;
import com.flink.wudy.window.tumbling.model.InputModel;
import com.flink.wudy.window.tumbling.model.OutputModel;
import com.flink.wudy.window.tumbling.source.ProductSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 计算每种商品1分钟内累计销售额(事件时间语义)
 *
 * 水位线:
 * 有序流： 对于有序流而言，只考虑 timestamp 而不考虑 watermark，是因为当前为有序流的前提下，把时间戳提取出来就可以作为水位线使用，没有必要再去指定水位线的生成策略。所以只需要提供一个 提取时间戳的方法 就可以
 */
public class TumblingWindowExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(ParameterTool.fromArgs(args).getConfiguration());
        // TODO:通过参数传入
        env.setParallelism(1);
        DataStreamSource<InputModel> source = env.addSource(new ProductSource());
        WatermarkStrategy<InputModel> watermarkStrategy = WatermarkStrategy
                // 周期性的生成水位线 有序流：forMonotonousTimestamps()  , 无序流：forBoundedOutOfOrderness()
                .<InputModel>forMonotonousTimestamps()
                // 从数据中获取时间戳作为事件事件语义下的时间戳
                .withTimestampAssigner((e, lastRecordTimeStamp) -> e.getTimestamp());

        SingleOutputStreamOperator<OutputModel> transformation =
                // 事件时间语义下需要分配时间戳和Watermark
        source.assignTimestampsAndWatermarks(watermarkStrategy)
                // 商品分类
                .keyBy(i -> i.getProductId())
                // 1分钟的事件时间滚动窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(60)))

                // 计算每种商品1min窗口内的累计销售额
                .apply(new WindowFunction<InputModel, OutputModel, String, TimeWindow>() {
                    // String key:keyBy 方法返回的参数, 即productId
                    // TimeWindow window:TimeWindow 代表时间窗口，可以通过TimeWindow获取时间窗口的开始时间戳和结束时间戳

                    @Override
                    public void apply(String key, TimeWindow timeWindow, Iterable<InputModel> input, Collector<OutputModel> output) throws Exception {
                        //Iterable<InputModel> input:窗口内的数据集合（集合就是有界流，时间窗口操作会将无界流的数据转换为有界流的数据处理）
                        //Collector<OutputModel> output :使用Collector输出窗口计算的结果
                        String productId = key;
                        int minuteIncome = 0;
                        for (InputModel inputModel : input){
                            minuteIncome += inputModel.getIncome();
                        }
                        output.collect(OutputModel.builder()
                                .productId(productId)
                                .minuteIncome(minuteIncome)
                                .minuteStartTimestamp(timeWindow.getStart())
                                .build());
                    }
                });
       DataStreamSink<OutputModel> sink = transformation.print();
       env.execute("tumbling window");
    }
}
