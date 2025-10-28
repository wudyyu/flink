package com.flink.wudy.sink.strategy.forward.source;

import com.flink.wudy.sink.strategy.forward.model.InputModel;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class UserDefineSource extends RichParallelSourceFunction<InputModel> {

    private volatile boolean isCancel = false;

    @Override
    public void run(SourceContext<InputModel> sourceContext) throws Exception {
        while (!isCancel){
            sourceContext.collect(
                    InputModel.builder()
                            .indexOfSourceSubTask(getRuntimeContext().getIndexOfThisSubtask() + 1)
                            .build()
            );
            Thread.sleep(5000);
        }
    }

    @Override
    public void cancel() {
        this.isCancel = true;
    }
}
