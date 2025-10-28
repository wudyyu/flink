package com.flink.wudy.sink.kafka.source;

import com.flink.wudy.sink.kafka.model.CustomInputModel;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * 自定义数据源
 */
public class CustomSource extends RichSourceFunction<CustomInputModel> {
    @Override
    public void run(SourceContext<CustomInputModel> sourceContext) throws Exception {

//        List<String> productIds = Arrays.asList("Clothes","Shoes","Hat","plaything","Book","computer","socks","Glasses");
        List<String> productIds = Arrays.asList("Clothes","Shoes");
//        List<Integer> incomeList = Arrays.asList(10,20,30,40,50,60,70,80);
        List<Integer> incomeList = Arrays.asList(10,10,10);
        List<Integer> countList = Arrays.asList(1,1,1);
//        List<BigDecimal> price = Arrays.asList(BigDecimal.ONE, BigDecimal.valueOf(2),BigDecimal.valueOf(3));
        List<BigDecimal> price = Arrays.asList(BigDecimal.ONE);

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

    }
}
