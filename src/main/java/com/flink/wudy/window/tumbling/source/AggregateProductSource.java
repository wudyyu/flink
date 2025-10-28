package com.flink.wudy.window.tumbling.source;

import com.flink.wudy.window.tumbling.model.AggregateInputModel;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class AggregateProductSource extends RichParallelSourceFunction<AggregateInputModel> {

    @Override
    public void run(SourceContext<AggregateInputModel> sourceContext) throws Exception {
        AggregateInputModel aggregateInputModel1 = new AggregateInputModel("商品1", 10L, 1728489603000L); // 2024-10-10 00:00:03
        AggregateInputModel aggregateInputModel2 = new AggregateInputModel("商品1", 10L,1728489613000L); // 2024-10-10 00:00:13
        AggregateInputModel aggregateInputModel3 = new AggregateInputModel("商品2", 10L,1728489623000L); // 2024-10-10 00:00:23
        AggregateInputModel aggregateInputModel4 = new AggregateInputModel("商品2", 20L,1728489670000L); // 2024-10-10 00:01:10
        AggregateInputModel aggregateInputModel5 = new AggregateInputModel("商品2", 20L,1728489775000L); // 2024-10-10 00:02:55
        AggregateInputModel aggregateInputModel6 = new AggregateInputModel("商品1", 30L,1728489783000L); // 2024-10-10 00:03:03
        AggregateInputModel aggregateInputModel7 = new AggregateInputModel("商品1", 10L,1728489805000L); // 2024-10-10 00:03:25
        AggregateInputModel aggregateInputModel8 = new AggregateInputModel("商品1", 20L,1728489805000L); // 2024-10-10 00:03:25
        AggregateInputModel aggregateInputModel9 = new AggregateInputModel("商品2", 30L,1728489806000L); // 2024-10-10 00:03:26
        AggregateInputModel aggregateInputModel10 = new AggregateInputModel("商品2", 50L,1728489806000L); // 2024-10-10 00:03:26

        sourceContext.collect(aggregateInputModel1);
        sourceContext.collect(aggregateInputModel2);
        sourceContext.collect(aggregateInputModel3);
        sourceContext.collect(aggregateInputModel4);
        sourceContext.collect(aggregateInputModel5);
        sourceContext.collect(aggregateInputModel6);
        sourceContext.collect(aggregateInputModel7);
        sourceContext.collect(aggregateInputModel8);
        sourceContext.collect(aggregateInputModel9);
        sourceContext.collect(aggregateInputModel10);
    }

    @Override
    public void cancel() {

    }
}
