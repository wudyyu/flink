package com.flink.wudy.window.tumbling.source;

import com.flink.wudy.window.tumbling.model.ReduceInputModel;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class ReduceProductSource extends RichParallelSourceFunction<ReduceInputModel> {

    private volatile boolean isCancel = false;

    @Override
    public void run(SourceContext<ReduceInputModel> sourceContext) throws Exception {
        ReduceInputModel reduceInputModel1 = new ReduceInputModel("商品1", 10L, 1728489603000L); // 2024-10-10 00:00:03
        ReduceInputModel reduceInputModel2 = new ReduceInputModel("商品1", 10L,1728489613000L); // 2024-10-10 00:00:13
        ReduceInputModel reduceInputModel3 = new ReduceInputModel("商品2", 10L,1728489623000L); // 2024-10-10 00:00:23
        ReduceInputModel reduceInputModel4 = new ReduceInputModel("商品2", 20L,1728489670000L); // 2024-10-10 00:01:10
        ReduceInputModel reduceInputModel5 = new ReduceInputModel("商品2", 20L,1728489775000L); // 2024-10-10 00:02:55
        ReduceInputModel reduceInputModel6 = new ReduceInputModel("商品1", 30L,1728489783000L); // 2024-10-10 00:03:03
        ReduceInputModel reduceInputModel7 = new ReduceInputModel("商品1", 10L,1728489805000L); // 2024-10-10 00:03:25
        ReduceInputModel reduceInputModel8 = new ReduceInputModel("商品1", 20L,1728489805000L); // 2024-10-10 00:03:25
        ReduceInputModel reduceInputModel9 = new ReduceInputModel("商品2", 30L,1728489806000L); // 2024-10-10 00:03:26
        ReduceInputModel reduceInputModel10 = new ReduceInputModel("商品2", 50L,1728489806000L); // 2024-10-10 00:03:26

        sourceContext.collect(reduceInputModel1);
        sourceContext.collect(reduceInputModel2);
        sourceContext.collect(reduceInputModel3);
        sourceContext.collect(reduceInputModel4);
        sourceContext.collect(reduceInputModel5);
        sourceContext.collect(reduceInputModel6);
        sourceContext.collect(reduceInputModel7);
        sourceContext.collect(reduceInputModel8);
        sourceContext.collect(reduceInputModel9);
        sourceContext.collect(reduceInputModel10);
    }

    @Override
    public void cancel() {
        this.isCancel = true;
    }
}
