package com.flink.wudy.valueState.source;

import com.flink.wudy.valueState.model.InputModel;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class InputModelSource extends RichParallelSourceFunction<InputModel> {
    @Override
    public void run(SourceContext<InputModel> sourceContext) throws Exception {

        List<String> productIds = Arrays.asList("10086", "32578");
        List<Long> incomes = Arrays.asList(3L,5L,7L,8L,9L);

        Random random = new Random();
        for (int i=1; i<=20; i++){
            InputModel inputModel = new InputModel();
            inputModel.setProductId(productIds.get(random.nextInt(productIds.size())));
            inputModel.setIncome(incomes.get(random.nextInt(incomes.size())));
            Thread.sleep(1000);
            inputModel.setTimestamp(System.currentTimeMillis());
            sourceContext.collect(inputModel);
        }

    }

    @Override
    public void cancel() {

    }
}
