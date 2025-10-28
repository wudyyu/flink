package com.flink.wudy.demo.flatMap.source;

import com.flink.wudy.demo.flatMap.models.FlatMapInputModel;
import com.flink.wudy.demo.flatMap.models.LogModel;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * FlatMap = Map + Filter
 * 建议使用FlatMap， 因为FlatMap仅有一个算子，性能更优，而Map + Filter 是两个算子
 */
public class FlatMapDefineSource implements ParallelSourceFunction<FlatMapInputModel> {

    private volatile  boolean isCancel = false;

    @Override
    public void run(SourceContext<FlatMapInputModel> sourceContext) throws Exception {
        // 手动构造数据
        FlatMapInputModel model1 = new FlatMapInputModel("wudy", new LogModel("page1", "button1"));
        FlatMapInputModel model2 = new FlatMapInputModel("wudy", new LogModel("page1", "button2"));
        FlatMapInputModel model3 = new FlatMapInputModel("wudy", new LogModel("page2", "button1"));

        FlatMapInputModel model4 = new FlatMapInputModel("peter", new LogModel("page1", "button3"));
        FlatMapInputModel model5 = new FlatMapInputModel("peter", new LogModel("page2", "button4"));
        FlatMapInputModel model6 = new FlatMapInputModel("peter", new LogModel("page2", "button2"));

        List<FlatMapInputModel> inputModels = Arrays.asList(model1, model2, model3, model4, model5, model6);

        Random random = new Random();
        while (!this.isCancel){
            int index = random.nextInt(5);
            sourceContext.collect(inputModels.get(index));
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        this.isCancel = true;
    }
}
