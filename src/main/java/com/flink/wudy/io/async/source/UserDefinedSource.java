package com.flink.wudy.io.async.source;

import com.flink.wudy.sink.kafka.model.CustomInputModel;
import com.flink.wudy.sink.strategy.forward.model.InputModel;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Random;


public class UserDefinedSource extends RichParallelSourceFunction<CustomInputModel> {

    private volatile boolean isCancel = false;

    @Override
    public void run(SourceContext<CustomInputModel> sourceContext) throws Exception {
        List<String> productIds = Arrays.asList("book","computer","food","car");
        List<Integer> incomeList = Arrays.asList(12,22,32);
        List<Integer> countList = Arrays.asList(15,25,35);
        List<BigDecimal> price = Arrays.asList(BigDecimal.ONE, BigDecimal.valueOf(2),BigDecimal.valueOf(3));

        Random random = new Random();
        for (int i = 0; i < 5; i++) {
            CustomInputModel customInputModel = new CustomInputModel();
            customInputModel.setProductId(productIds.get(random.nextInt(productIds.size())));
            customInputModel.setIncome(incomeList.get(random.nextInt(incomeList.size())));
            customInputModel.setCount(countList.get(random.nextInt(countList.size())));
            customInputModel.setAvgPrice(price.get(random.nextInt(price.size())));

            sourceContext.collect(customInputModel);
        }
    }

    @Override
    public void cancel() {
        this.isCancel = true;
    }
}
