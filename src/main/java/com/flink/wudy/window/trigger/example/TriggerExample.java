package com.flink.wudy.window.trigger.example;

import com.flink.wudy.window.trigger.model.ProductInputModel;
import com.flink.wudy.window.trigger.model.ProductOutputModel;
import com.flink.wudy.window.trigger.source.ProductSource;
import com.flink.wudy.window.tumbling.model.AggregateOutputModel;
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
 * 窗口触发器
 *
 * >Flink预置的 滚动窗口、滑动窗口、会话窗口分配器中提供的默认窗口触发器
 * 1.ProcessingTimeTrigger: 处理时间语义的触发器，会为窗口注册处理时间的定时器。当处理时间达到处理时间定时器的时间时，触发窗口计算
 * 2.EventTimeTrigger: 事件时间语义的触发器，会为窗口注册事件时间的定时器。当Watermark达到事件时间定时器的时间时，触发窗口计算
 *
 * 除了上面两种常用的触发器外，还提供了以下6中窗口触发器
 * 3.ContinuousProcessingTimeTrigger.of(Time interval) : 处理时间语义下，按照interval间隔持续触发的触发器。以一个5min大小滚动窗口为例，
 *   当我们设置ContinuousProcessingTimeTrigger.of(Time.minutes(1))的窗口触发器后，假设窗口为[09:00:00, 09:05:00], n那么盖茨会在
 *   09:01:00、09:02:00、09:03:00、09:04:00、09:05:00 分别触发一次窗口内数据计算，这种触发器被称为 提前触发器
 * 4.ContinuousEventTimeTrigger.of(Time interval): 事件时间语义下(与3雷同)
 * 5.CounterTrigger.of(long maxCount): 计数触发器，在窗口条目数达到maxCount是触发计算，
 * 6.DeltaTrigger.of: 阈值触发器
 * 7.ProcessingTimeoutTrigger.of: 处理时间语义下的 超时触发器, 需要和其他触发器搭配使用。
 * > 以一个5min事件时间语义的滚动窗口为例，每天的最后一个窗口为[23:55:00, 24:00:00], 假设在当天23点之后旧没有数据产生了，Watermark无法达到窗口的结束时间，
 *   因此这个窗口会一直无法触发计算，假设直到第二天08:00:00才开始产生数据，那么当天的数据无法在当天计算完成，这时候我们使用 超时触发器，在最后一个窗口创建8min后
 *   (即为第二天的 00:03:00时刻)，如果Watermark还无法触发窗口计算，就通过处理时间触发这个窗口的计算
 *
 * >自定义窗口触发器:
 *  - 电商中计算每种商品每天的累计销售额、销量以及平均销售额为例，这是一个典型的时间窗口大小为1天的滚动窗口，可以使用TumblingEventTimeWIndows.of(Time.days(1), Time.hours(-8))来实现
 *    问题在于，如果以默认的EventTimeTrigger 触发器来计算，窗口算子在一整天中都只会在窗口中累计数据，只有到了0点才会触发计算，指标实时性很差
 *    我们希望：将窗口触发器由1天触发一次改为每分钟触发一次（提前触发）
 *
 */
public class TriggerExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(ParameterTool.fromArgs(args).getConfiguration());
        // TODO:通过参数传入
        env.setParallelism(1);

        DataStreamSource<ProductInputModel> source = env.addSource(new ProductSource());
        WatermarkStrategy<ProductInputModel> watermarkStrategy =
                WatermarkStrategy.<ProductInputModel>forMonotonousTimestamps()
                        .withTimestampAssigner((e, lastRecordTimeStamp) -> e.getTimestamp());

        AggregateFunction<ProductInputModel, Tuple2<ProductInputModel, Long>, ProductOutputModel> myAggFunction = new AggregateFunction<ProductInputModel, Tuple2<ProductInputModel, Long>, ProductOutputModel>() {
            @Override
            public Tuple2<ProductInputModel, Long> createAccumulator() {
                System.out.println("AggregateFunction的初始化累加器");
                return Tuple2.of(null, 0L);
            }

            @Override
            public Tuple2<ProductInputModel, Long> add(ProductInputModel inputModel, Tuple2<ProductInputModel, Long> acc) {
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
            public ProductOutputModel getResult(Tuple2<ProductInputModel, Long> acc) {
                System.out.println("AggregateFunction获取结果");
                return ProductOutputModel
                        .builder()
                        .productId(acc.f0.getProductId())
                        .windowStart(DateFormatUtils.format(new Date(acc.f0.getTimestamp()), "yyyy-MM-dd HH:mm:ss"))
                        .avgIncome(acc.f0.getIncome() / acc.f1)
                        .allCnt(acc.f1)
                        .allIncome(acc.f0.getIncome())
                        .build();
            }

            @Override
            public Tuple2<ProductInputModel, Long> merge(Tuple2<ProductInputModel, Long> acc1, Tuple2<ProductInputModel, Long> acc2) {
                // 滚动窗口 无需实现merge方法
                return null;
            }
        };

        DataStream<ProductOutputModel> transformation =
                source.assignTimestampsAndWatermarks(watermarkStrategy)
                        .keyBy(ProductInputModel::getProductId)
                        .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))
                        .trigger(EarlyFireEventTimeTrigger.of(Time.seconds(60)))
                        .aggregate(myAggFunction);


        DataStreamSink<ProductOutputModel> sink = transformation.print();
        env.execute("EarlyFireTimestampTrigger AggregateFunction");
    }
}
