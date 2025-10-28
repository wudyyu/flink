package com.flink.wudy.demo.reduce.source;

import com.flink.wudy.demo.reduce.models.ProductInputModel;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

public class ProductSource implements SourceFunction<ProductInputModel> {
    @Override
    public void run(SourceContext<ProductInputModel> sourceContext) throws Exception {
        ProductInputModel inputModel1 = new ProductInputModel("商品1", 10, 2, new BigDecimal(100));
        ProductInputModel inputModel2 = new ProductInputModel("商品1", 20, 4, new BigDecimal(200));

        ProductInputModel inputModel3 = new ProductInputModel("商品2", 30, 4, new BigDecimal(200));
        ProductInputModel inputModel4 = new ProductInputModel("商品2", 10, 10, new BigDecimal(500));

        ProductInputModel inputModel5 = new ProductInputModel("商品3", 50, 10, new BigDecimal(600));
        ProductInputModel inputModel6 = new ProductInputModel("商品3", 45, 20, new BigDecimal(1000));

        List<ProductInputModel> inputModelList = Arrays.asList(inputModel1, inputModel2, inputModel3, inputModel4, inputModel5, inputModel6);

        for (ProductInputModel inputModel : inputModelList) {
            sourceContext.collect(inputModel);
        }
    }

    @Override
    public void cancel() {

    }
}
