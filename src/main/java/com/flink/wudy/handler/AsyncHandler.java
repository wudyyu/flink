package com.flink.wudy.handler;

import com.alibaba.fastjson.JSON;
import com.flink.wudy.io.async.model.UserClickMsg;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.concurrent.*;

public class AsyncHandler extends RichAsyncFunction<UserClickMsg, String> {

    private transient ThreadPoolExecutor threadPool;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        threadPool = new ThreadPoolExecutor(10, 100, 3000, TimeUnit.MICROSECONDS,
                new ArrayBlockingQueue<>(1000), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("async-handle-" + RandomUtils.nextInt(0,100));
                return thread;
            }
        });
    }

    @Override
    public void close() throws Exception {
        super.close();
        threadPool.shutdown();
    }

    @Override
    public void asyncInvoke(UserClickMsg userClickMsg, ResultFuture<String> resultFuture) throws Exception {
        CompletableFuture.runAsync(() -> {
            System.out.println("异步处理数据:" + Thread.currentThread().getName() + "|" + JSON.toJSONString(userClickMsg));
            //这里面你可以做自定义的业务操作，例如es查询，mongodb查询等等
            resultFuture.complete(Collections.singleton("success"));
        });
    }
}
