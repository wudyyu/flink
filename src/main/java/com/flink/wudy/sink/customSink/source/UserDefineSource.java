package com.flink.wudy.sink.customSink.source;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class UserDefineSource implements SourceFunction<String> {

    private volatile  boolean isCancel = false;

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {

        List<String> stringList = Arrays.asList("wudy.yu", "peter.li", "timo.peng");

        while (!this.isCancel){
            Random random = new Random();
            while (!this.isCancel){
                int index = random.nextInt(3);
                sourceContext.collect(stringList.get(index));
                Thread.sleep(1000);
            }
        }

    }

    @Override
    public void cancel() {
        this.isCancel = true;
    }
}


