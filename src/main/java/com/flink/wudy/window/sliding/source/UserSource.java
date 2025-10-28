package com.flink.wudy.window.sliding.source;

import com.flink.wudy.window.sliding.model.InputModel;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class UserSource extends RichParallelSourceFunction<InputModel> {
    private volatile boolean isCancel = false;

    @Override
    public void run(SourceContext<InputModel> sourceContext) throws Exception {
        InputModel inputModel1 = new InputModel(2, 1728489663000L); // 2024-10-10 00:01:03
        InputModel inputModel2 = new InputModel(3, 1728489724000L); // 2024-10-10 00:02:04
        InputModel inputModel3 = new InputModel(4, 1728489724000L); // 2024-10-10 00:02:04
        InputModel inputModel4 = new InputModel(5, 1728489724000L); // 2024-10-10 00:02:04
        InputModel inputModel5 = new InputModel(6, 1728489785000L); // 2024-10-10 00:03:05
        InputModel inputModel6 = new InputModel(6, 1728489845000L); // 2024-10-10 00:04:05

        sourceContext.collect(inputModel1);
        sourceContext.collect(inputModel2);
        sourceContext.collect(inputModel3);
        sourceContext.collect(inputModel4);
        sourceContext.collect(inputModel5);
        sourceContext.collect(inputModel6);
    }

    @Override
    public void cancel() {
        this.isCancel = true;
    }
}
