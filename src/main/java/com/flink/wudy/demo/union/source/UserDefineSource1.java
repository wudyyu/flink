package com.flink.wudy.demo.union.source;

import com.flink.wudy.demo.flatMap.models.FlatMapInputModel;
import com.flink.wudy.demo.flatMap.models.LogModel;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Arrays;
import java.util.List;

public class UserDefineSource1 implements ParallelSourceFunction<FlatMapInputModel> {

    @Override
    public void run(SourceContext<FlatMapInputModel> sourceContext) throws Exception {
        // 手动构造数据
        FlatMapInputModel model1 = new FlatMapInputModel("wudy", new LogModel("page1", "button1"));
        FlatMapInputModel model2 = new FlatMapInputModel("wudy", new LogModel("page1", "button2"));
        FlatMapInputModel model3 = new FlatMapInputModel("wudy", new LogModel("page2", "button1"));

        List<FlatMapInputModel> inputModels = Arrays.asList(model1, model2, model3);

        for (FlatMapInputModel inputModel : inputModels) {
            sourceContext.collect(inputModel);
        }
    }

    @Override
    public void cancel() {

    }
}
