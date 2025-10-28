package com.flink.wudy.demo.keyBy.source;

import com.flink.wudy.demo.keyBy.models.ProductInputModel;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Arrays;
import java.util.List;

public class ProductSource implements ParallelSourceFunction<ProductInputModel> {

    @Override
    public void run(SourceContext<ProductInputModel> sourceContext) throws Exception {
        ProductInputModel productInputModel1 = new ProductInputModel("商品1", 10);
        ProductInputModel productInputModel2 = new ProductInputModel("商品2", 20);
        ProductInputModel productInputModel3 = new ProductInputModel("商品1", 30);
        ProductInputModel productInputModel4 = new ProductInputModel("商品2", 40);

        List<ProductInputModel> productInputModels = Arrays.asList(productInputModel1, productInputModel2,productInputModel3,productInputModel4);
        for (ProductInputModel productModel : productInputModels) {
            sourceContext.collect(productModel);
        }

    }

    @Override
    public void cancel() {

    }
}
