package com.flink.wudy.window.tumbling.source;

import com.flink.wudy.window.tumbling.model.ProcessInputModel;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class ProcessProductSource extends RichParallelSourceFunction<ProcessInputModel> {

    private volatile boolean isCancel = false;

    @Override
    public void run(SourceContext<ProcessInputModel> sourceContext) throws Exception {
        ProcessInputModel inputModel1 = new ProcessInputModel(1, "商品1", 1728489603000L); // 2024-10-10 00:00:03
        ProcessInputModel inputModel2 = new ProcessInputModel(1, "商品1", 1728489613000L); // 2024-10-10 00:00:13
        ProcessInputModel inputModel3 = new ProcessInputModel(1, "商品2", 1728489623000L); // 2024-10-10 00:00:23
        ProcessInputModel inputModel4 = new ProcessInputModel(1, "商品2", 1728489670000L); // 2024-10-10 00:01:10

        ProcessInputModel inputModel5 = new ProcessInputModel(1, "商品1", 1728489775000L); // 2024-10-10 00:02:55

        ProcessInputModel inputModel6 = new ProcessInputModel(1, "商品1", 1728489783000L); // 2024-10-10 00:03:03
        ProcessInputModel inputModel7 = new ProcessInputModel(1, "商品2", 1728489805000L); // 2024-10-10 00:03:25
        ProcessInputModel inputModel8 = new ProcessInputModel(1, "商品1", 1728489805000L); // 2024-10-10 00:03:25
        ProcessInputModel inputModel9 = new ProcessInputModel(1, "商品2", 1728489806000L); // 2024-10-10 00:03:26
        ProcessInputModel inputModel10 = new ProcessInputModel(1, "商品1", 1728489806000L); // 2024-10-10 00:03:26

        sourceContext.collect(inputModel1);
        sourceContext.collect(inputModel2);
        sourceContext.collect(inputModel3);
        sourceContext.collect(inputModel4);
        sourceContext.collect(inputModel5);
        sourceContext.collect(inputModel6);
        sourceContext.collect(inputModel7);
        sourceContext.collect(inputModel8);
        sourceContext.collect(inputModel9);
        sourceContext.collect(inputModel10);
    }

    @Override
    public void cancel() {
        this.isCancel = true;
    }
}
