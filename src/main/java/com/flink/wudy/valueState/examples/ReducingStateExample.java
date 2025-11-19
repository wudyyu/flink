package com.flink.wudy.valueState.examples;

import com.flink.wudy.valueState.function.MySqlSinkFunction;
import com.flink.wudy.valueState.model.InputModel;
import com.flink.wudy.valueState.source.InputModelSource;
import com.flink.wudy.valueState.source.ProductSaleModelSource;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * ReducingState 与 AggregatingState的一个农业难过场景几乎相同，因此这里以ReducingState为例
 *
 * 案例： 电商场景计算每种商品的累计销售额
 * */

public class ReducingStateExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(ParameterTool.fromArgs(args).getConfiguration());
        env.setParallelism(2);
        // 启用检查点功能，触发间隔为10秒，设定了精确一次（Exactly-Once）的语义保证
        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);

        DataStreamSource<InputModel> productStream = env.addSource(new InputModelSource(), "product_sale_name");

        SingleOutputStreamOperator<InputModel> transformation =
        productStream.keyBy(InputModel::getProductId)
                .flatMap(new RichFlatMapFunction<InputModel, InputModel>() {
                    private ReducingStateDescriptor<Long> cumulateStateDescriptor = new ReducingStateDescriptor<Long>("cumulate income", new ReduceFunction<Long>() {
                        @Override
                        public Long reduce(Long value1, Long value2) throws Exception {
                            return value1 + value2;
                        }
                    }, Long.class);

                    private transient ReducingState<Long> reducingState;

            @Override
            public void flatMap(InputModel inputModel, Collector<InputModel> collector) throws Exception {
                // 计算累计销售额
                this.reducingState.add(inputModel.getIncome());
                inputModel.setIncome(this.reducingState.get());
                collector.collect(inputModel);
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                this.reducingState = this.getRuntimeContext().getReducingState(cumulateStateDescriptor);
            }

        });
        DataStreamSink<InputModel> productSinkStream = transformation.addSink(new MySqlSinkFunction(10)).name("product_sale_sink");

        env.execute("product sale");


    }
}
