package com.flink.wudy.valueState.source;

import com.flink.wudy.valueState.model.InputModel;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class ProductSaleModelSource extends RichParallelSourceFunction<InputModel> {

    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<InputModel> sourceContext) throws Exception {
        List<String> productIds = Arrays.asList("10086", "32578");
        List<Long> incomes = Arrays.asList(2L,4L,6L,8L,10L);

        Random random = new Random();
        for (int i=1; i<=20 && isRunning; i++){
            InputModel inputModel = new InputModel();
            inputModel.setProductId(productIds.get(random.nextInt(productIds.size())));
            inputModel.setIncome(incomes.get(random.nextInt(incomes.size())));
            inputModel.setTimestamp(System.currentTimeMillis());
            sourceContext.collect(inputModel);
        }

    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
