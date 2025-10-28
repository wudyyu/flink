package com.flink.wudy.demo.union.source;

import com.flink.wudy.demo.flatMap.models.FlatMapInputModel;
import com.flink.wudy.demo.flatMap.models.LogModel;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Arrays;
import java.util.List;

public class UserDefineSource2 implements ParallelSourceFunction<FlatMapInputModel> {

    @Override
    public void run(SourceContext<FlatMapInputModel> sourceContext) throws Exception {
        // 手动构造数据
        FlatMapInputModel model4 = new FlatMapInputModel("peter", new LogModel("page1", "button3"));
        FlatMapInputModel model5 = new FlatMapInputModel("peter", new LogModel("page2", "button4"));
        FlatMapInputModel model6 = new FlatMapInputModel("peter", new LogModel("page2", "button2"));

        List<FlatMapInputModel> inputModels = Arrays.asList(model4, model5, model6);

        for (FlatMapInputModel inputModel : inputModels) {
            sourceContext.collect(inputModel);
        }
    }

    @Override
    public void cancel() {

    }
}
