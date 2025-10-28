package com.flink.wudy.demo.keyBy.example;


import com.flink.wudy.demo.keyBy.models.ProductInputModel;
import com.flink.wudy.demo.keyBy.source.ProductSource;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Max、 Min、Sum案例
 * 通过keyBy对数据进行分组不是目的，最终目的是将分组后的数据进行聚合计算得到某一段时间内的统计结果，这里仅以Sum分析案例
 * keyBy/Sum算子在计算时会涉及Flink的状态机制，keyBy/Sum算子利用状态 对每一个商品的销售额进行累计求和计算
 */
public class SumExamples {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(ParameterTool.fromArgs(args).getConfiguration());
        env.setParallelism(2);

        DataStreamSource<ProductInputModel> source = env.addSource(new ProductSource());

        SingleOutputStreamOperator<ProductInputModel> transformation =
        source.keyBy(new KeySelector<ProductInputModel, String>() {

            @Override
            public String getKey(ProductInputModel productInputModel) throws Exception {
                return productInputModel.getProductId();
            }
        }).sum("income");

        DataStreamSink<ProductInputModel> sink = transformation.print();
        env.execute("Flink Sum Example");
    }
}
