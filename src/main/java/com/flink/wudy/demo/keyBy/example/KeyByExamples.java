package com.flink.wudy.demo.keyBy.example;

import com.flink.wudy.demo.keyBy.models.ProductInputModel;
import com.flink.wudy.demo.keyBy.source.ProductSource;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 分组聚合：
 * 电商场景：计算店铺中商品的实时销量、店铺的累计销量
 * * 按照商品种类进行分组，然后聚合计算每种商品的累计销量
 * * 按照店铺进行分组，然后聚合计算每个店铺的累计销量
 *
 * 数据分析场景：计算不同年龄段、不同性别用户的App累计使用时常
 * * 按照年龄段进行分组，然后聚合计算每个年龄段的App累计使用时长
 * * 按照性别进行分组，然后聚合计算每个性别的App累计使用时长
 *
 * keyBy操作将一条输入流DataStream转化为KeyedStream，KeyedStream继承自DataStream
 * keyBy操作和Union、Connect操作类似，控制的是数据流在上下游算子间的传输方式，而非对数据流种的数据的计算操作
 * KeyedStream<T,K>, T代表输入数据类型， K代表数据分组键
 *
 */
public class KeyByExamples {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(ParameterTool.fromArgs(args).getConfiguration());
        env.setParallelism(2);

        DataStream<ProductInputModel> source = env.addSource(new ProductSource());
        KeyedStream<ProductInputModel, String> transformation =
        source.keyBy(new KeySelector<ProductInputModel, String>() {
            @Override
            public String getKey(ProductInputModel productInputModel) throws Exception {
                return productInputModel.getProductId();
            }
        });

        DataStreamSink<ProductInputModel> sink = transformation.print();
        env.execute("Flink KeyBy Example");
    }
}
