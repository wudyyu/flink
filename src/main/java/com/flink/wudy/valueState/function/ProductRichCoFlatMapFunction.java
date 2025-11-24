package com.flink.wudy.valueState.function;

import com.flink.wudy.valueState.model.ConfigModel;
import com.flink.wudy.valueState.model.ProductLogModel;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

public class ProductRichCoFlatMapFunction extends RichCoFlatMapFunction<ProductLogModel, ConfigModel, ProductLogModel> {

    private volatile ConfigModel configModel;

    @Override
    public void flatMap1(ProductLogModel value, Collector<ProductLogModel> collector) throws Exception {
        if (null != this.configModel){
            // 输出id在configModel中的订单数据
            if (configModel.getProductIds().contains(value.getProductId())){
                collector.collect(value);
            }
        }
    }

    @Override
    public void flatMap2(ConfigModel value, Collector<ProductLogModel> collector) throws Exception {
        this.configModel = value;
    }
}

