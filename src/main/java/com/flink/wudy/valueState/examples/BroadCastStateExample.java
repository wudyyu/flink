package com.flink.wudy.valueState.examples;

import com.flink.wudy.valueState.function.ConfigSourceFunction;
import com.flink.wudy.valueState.function.ProductRichCoFlatMapFunction;
import com.flink.wudy.valueState.model.ConfigModel;
import com.flink.wudy.valueState.model.ProductLogModel;
import com.flink.wudy.valueState.source.ProductLogSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 广播状态
 * > 是一种特殊的算子状态，作用域也是算子的单个SubTask，算子的每一个SubTask上的广播状态值必须完全一样
 *
 * > 案例分析
 *   商品销售订单的日志流(userId、productId)，需要过滤出其中一批productId的销售订单日志(Source -> Filter -> Sink)
 *   在Filter算子中定义要过滤的productId集合，并实现数据过滤的逻辑
 *   问题: productId集可能会变化，我们考虑将其存放配置中心Nacos,Filter算子中的所有SubTask会定时访问远程配置中心，该方案存在一个缺点：
 *         如果Filter算子并行度增加，那么会有较多的SubTask连接到Nacos配置中心，影响配置中心服务的稳定性
 *   解决思路：一个作业中只需要一个SubTask去访问配置中心，然后将读取到的规则发送给其他的SubTask，可以降低对配置中心的压力
 *
 *
 */
public class BroadCastStateExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(ParameterTool.fromArgs(args).getConfiguration());
        DataStream<ConfigModel> configStream = env.addSource(new ConfigSourceFunction())
                .setParallelism(1) //设置算子并行度为1，数据传输策略 broadcast
                .broadcast();
        SingleOutputStreamOperator<ProductLogModel> transformation = env.addSource(new ProductLogSource())
                .connect(configStream) // 将广播流和订单流连接起来
                .flatMap(new ProductRichCoFlatMapFunction())
                .name("双流join再过滤");
        transformation.print().name("输出数据");
        env.execute("广播任务执行");
    }
}
