package com.flink.wudy.valueState.source;

import com.flink.wudy.valueState.model.ProductLogModel;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class ProductLogSource extends RichParallelSourceFunction<ProductLogModel> {
    private volatile boolean isCancel = false;

    @Override
    public void run(SourceContext<ProductLogModel> sourceContext) throws Exception {
        List<Long> productIds = Arrays.asList(10086L, 32578L, 26351L,87152L,98125L,52412L,88612L,44132L);
        Random random = new Random();

        while(!this.isCancel){
            Long productId = productIds.get(random.nextInt(productIds.size()));
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            sourceContext.collect(
                    ProductLogModel.builder()
                            .productId(productId)
                            .log(LocalDateTime.now().format(formatter))
                            .build()
            );
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        this.isCancel = true;
    }
}
