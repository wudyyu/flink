package com.flink.wudy.window.trigger.source;

import com.flink.wudy.window.trigger.model.ProductInputModel;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class ProductSource extends RichParallelSourceFunction<ProductInputModel> {
    @Override
    public void run(SourceContext<ProductInputModel> sourceContext) throws Exception {
        ProductInputModel productInputModel1 = new ProductInputModel("商品1", 10L, 1728489603000L); // 2024-10-10 00:00:03
        ProductInputModel productInputModel2 = new ProductInputModel("商品1", 10L,1728489613000L); // 2024-10-10 00:00:13
        ProductInputModel productInputModel3 = new ProductInputModel("商品2", 10L,1728489623000L); // 2024-10-10 00:00:23
        ProductInputModel productInputModel4 = new ProductInputModel("商品2", 20L,1728489670000L); // 2024-10-10 00:01:10
        ProductInputModel productInputModel5 = new ProductInputModel("商品2", 20L,1728489775000L); // 2024-10-10 00:02:55
        ProductInputModel productInputModel6 = new ProductInputModel("商品1", 30L,1728489783000L); // 2024-10-10 00:03:03
        ProductInputModel productInputModel7 = new ProductInputModel("商品1", 10L,1728489805000L); // 2024-10-10 00:03:25
        ProductInputModel productInputModel8 = new ProductInputModel("商品1", 20L,1728489805000L); // 2024-10-10 00:03:25
        ProductInputModel productInputModel9 = new ProductInputModel("商品2", 30L,1728489806000L); // 2024-10-10 00:03:26
        ProductInputModel productInputModel10 = new ProductInputModel("商品2", 50L,1728489806000L); // 2024-10-10 00:03:26
        ProductInputModel productInputModel11 = new ProductInputModel("商品1", 20L,1728489868000L); // 2024-10-10 00:04:28
        ProductInputModel productInputModel12 = new ProductInputModel("商品1", 20L,1728489948000L); // 2024-10-10 00:05:48

        sourceContext.collect(productInputModel1);
        sourceContext.collect(productInputModel2);
        sourceContext.collect(productInputModel3);
        sourceContext.collect(productInputModel4);
        sourceContext.collect(productInputModel5);
        sourceContext.collect(productInputModel6);
        sourceContext.collect(productInputModel7);
        sourceContext.collect(productInputModel8);
        sourceContext.collect(productInputModel9);
        sourceContext.collect(productInputModel10);
        sourceContext.collect(productInputModel11);
        sourceContext.collect(productInputModel12);
    }

    @Override
    public void cancel() {

    }
}
