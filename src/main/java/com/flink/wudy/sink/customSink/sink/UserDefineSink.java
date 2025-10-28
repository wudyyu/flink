package com.flink.wudy.sink.customSink.sink;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class UserDefineSink implements SinkFunction<String> {

    @Override
    public void invoke(String value, Context context) throws Exception {
        SinkFunction.super.invoke(value, context);
        System.out.println("想外部数据汇引擎写入数据= " + value);
    }
}
