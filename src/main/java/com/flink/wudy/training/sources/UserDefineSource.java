package com.flink.wudy.training.sources;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

public class UserDefineSource  implements ParallelSourceFunction<String> {

    private volatile  boolean isCancel = false;

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        int i = 0;
        while (!this.isCancel){
            i++;
            sourceContext.collect("I am a Writer " + i);
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        this.isCancel = true;
    }
}
