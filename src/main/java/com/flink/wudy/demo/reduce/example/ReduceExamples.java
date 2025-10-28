package com.flink.wudy.demo.reduce.example;

import com.flink.wudy.demo.reduce.models.ProductInputModel;
import com.flink.wudy.demo.reduce.source.ProductSource;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * 通过Reduce操作自定义数据聚合计算的逻辑
 * ReduceFunction<T> 接口的 T reduce(T acc, T in) 方法，acc代表该分组的历史聚合结果，in代表该分组种新的输入数据,方法返回值代表这一次规约聚合计算之后的结果
 * 案例：计算每种商品的累计销售额、累计销量、平均销售额，输入数据是商品的销售记录 ProductInputModel
 * 实现逻辑：按照productId分组，通过reduce操作计算出累计销售额、累计销量，然后做除法运算得到每种商品的平均销售额，最终输出结果
 */
public class ReduceExamples {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(ParameterTool.fromArgs(args).getConfiguration());
        env.setParallelism(2);

        DataStreamSource<ProductInputModel> source = env.addSource(new ProductSource());

        SingleOutputStreamOperator<ProductInputModel> transformation =
//        DataStream<ProductInputModel> transformation =
        source.keyBy(new KeySelector<ProductInputModel, String>() {
            @Override
            public String getKey(ProductInputModel productInputModel) throws Exception {
                return productInputModel.getProductId();
            }
        }).reduce(new ReduceFunction<ProductInputModel>() {
            @Override
            public ProductInputModel reduce(ProductInputModel acc, ProductInputModel in) throws Exception {
                // 计算累计销售额
                acc.setIncome(acc.getIncome() + in.getIncome());
                // 计算累计销量
                acc.setCount(acc.getCount() + in.getCount());
                // 计算平均销售额
                acc.setAvgPrice(new BigDecimal(acc.getIncome()).divide(new BigDecimal(acc.getCount()), RoundingMode.UP));
                return acc;
            }
        });

        DataStreamSink<ProductInputModel> sink = transformation.print();
        env.execute("Flink Reduce Example");
    }
}
