package com.flink.wudy.valueState.examples;

import com.flink.wudy.valueState.function.MySqlSinkFunction;
import com.flink.wudy.valueState.function.ProductKeyedRichMapFunction;
import com.flink.wudy.valueState.function.ProductMapStateFunction;
import com.flink.wudy.valueState.model.InputModel;
import com.flink.wudy.valueState.model.UserProductModel;
import com.flink.wudy.valueState.source.ProductSaleModelSource;
import com.flink.wudy.valueState.source.UserProductModelSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 案例：过滤某个用户第一次购买某种商品的记录
 *
 * */
public class MapStateExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(ParameterTool.fromArgs(args).getConfiguration());
        env.setParallelism(2);

        // 启用检查点功能，触发间隔为10秒，设定了精确一次（Exactly-Once）的语义保证
        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);

        DataStreamSource<UserProductModel> productSourceStream = env.addSource(new UserProductModelSource(), "user_product_source");
        SingleOutputStreamOperator<UserProductModel> sinkStream = productSourceStream.keyBy(UserProductModel::getProductId).flatMap(new ProductMapStateFunction()).name("Product Sale Map Task");
        sinkStream.print().name("print out product sale");
        env.execute("user product");
    }
}
