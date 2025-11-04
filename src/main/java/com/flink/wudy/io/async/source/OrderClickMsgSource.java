package com.flink.wudy.io.async.source;

import com.flink.wudy.io.async.model.UserClickMsg;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class OrderClickMsgSource extends RichParallelSourceFunction<UserClickMsg> {

    @Override
    public void run(SourceContext<UserClickMsg> sourceContext) throws Exception {
        List<String> platforms = Arrays.asList("pdd");
        List<Long> userList = Arrays.asList(1L, 2L, 3L);
        List<Long> goodsList = Arrays.asList(101L, 201L, 301L);

        List<Integer> actionList = Arrays.asList(1, 2, 3, 4);

        Random random = new Random();
        for (int i = 0; i < 5; i++) {
            UserClickMsg userClickMsg = new UserClickMsg();
            userClickMsg.setPlatform(platforms.get(random.nextInt(platforms.size())));
            userClickMsg.setUserId(userList.get(random.nextInt(userList.size())));
            userClickMsg.setAction(actionList.get(random.nextInt(actionList.size())));
            userClickMsg.setGoodId(goodsList.get(random.nextInt(goodsList.size())));
            sourceContext.collect(userClickMsg);
        }
    }

    @Override
    public void cancel() {

    }
}
