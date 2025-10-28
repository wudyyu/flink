package com.flink.wudy.window.session.source;


import com.flink.wudy.window.session.model.InputModel;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class UserSource extends RichParallelSourceFunction<InputModel> {
    private volatile boolean isCancel = false;

    @Override
    public void run(SourceContext<InputModel> sourceContext) throws Exception {
        InputModel inputModel1 = new InputModel(2, 1728489663000L); // 2024-10-10 00:01:03
        InputModel inputModel2 = new InputModel(2, 1728489902000L); // 2024-10-10 00:05:02
        InputModel inputModel3 = new InputModel(2, 1728492663000L); // 2024-10-10 00:51:03
        InputModel inputModel4 = new InputModel(2, 1728492723000L); // 2024-10-10 00:52:03
        InputModel inputModel5 = new InputModel(2, 1728492723000L); // 2024-10-10 00:52:03

        sourceContext.collect(inputModel1);
        sourceContext.collect(inputModel2);
        sourceContext.collect(inputModel3);
        sourceContext.collect(inputModel4);
        sourceContext.collect(inputModel5);
    }

    @Override
    public void cancel() {
        this.isCancel = true;
    }
}
