package com.flink.wudy.clickhouse.transfer;

import com.alibaba.fastjson.JSON;
import com.flink.wudy.clickhouse.model.BillEntityModel;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class BillFlatMapFunction implements FlatMapFunction<String, BillEntityModel> {

    @Override
    public void flatMap(String s, Collector<BillEntityModel> collector) throws Exception {
        BillEntityModel billEntityModel = JSON.parseObject(s, BillEntityModel.class);
        collector.collect(billEntityModel);
    }
}
