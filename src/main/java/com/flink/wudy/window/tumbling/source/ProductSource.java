package com.flink.wudy.window.tumbling.source;

import com.flink.wudy.window.tumbling.model.InputModel;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class ProductSource extends RichParallelSourceFunction<InputModel> {

    private volatile boolean isCancel = false;

    @Override
    public void run(SourceContext<InputModel> sourceContext) throws Exception {
        InputModel productModel1 = new InputModel("Clothes", 10, 1728489600000L); // 2024-10-10 00:00:00
        InputModel productModel2 = new InputModel("Clothes", 20, 1728489603000L); // 2024-10-10 00:00:03
        InputModel productModel3 = new InputModel("Books", 30, 1728489604000L); //   2024-10-10 00:00:04
        InputModel productModel4 = new InputModel("Shoes", 20, 1728489665000L); //   2024-10-10 00:01:05
        InputModel productModel5 = new InputModel("Clothes", 10, 1728489727000L); //   2024-10-10 00:02:07
        InputModel productModel6 = new InputModel("Shoes", 30, 1728489846000L); //   2024-10-10 00:04:06
        InputModel productModel7 = new InputModel("Shoes", 30, 1728489906000L); //   2024-10-10 00:05:06

        sourceContext.collect(productModel1);
        sourceContext.collect(productModel2);
        sourceContext.collect(productModel3);
        sourceContext.collect(productModel4);
        sourceContext.collect(productModel5);
        sourceContext.collect(productModel6);
        sourceContext.collect(productModel7);
    }

    @Override
    public void cancel() {
        this.isCancel = true;
    }
}
