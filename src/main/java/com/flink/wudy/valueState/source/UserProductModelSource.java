package com.flink.wudy.valueState.source;

import com.flink.wudy.valueState.model.UserProductModel;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class UserProductModelSource extends RichParallelSourceFunction<UserProductModel> {

    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<UserProductModel> sourceContext) throws Exception {
        List<String> productIds = Arrays.asList("10086", "32578");
        List<String> userIds = Arrays.asList("201108003118", "201108003119");

        Random random = new Random();
        for (int i=1; i<=20 && isRunning; i++){
            UserProductModel inputModel = new UserProductModel();
            inputModel.setProductId(productIds.get(random.nextInt(productIds.size())));
            inputModel.setUserId(userIds.get(random.nextInt(userIds.size())));
            inputModel.setTimestamp(System.currentTimeMillis());
            sourceContext.collect(inputModel);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
