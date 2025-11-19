package com.flink.wudy.valueState.function;

import com.flink.wudy.valueState.model.UserProductModel;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class ProductMapStateFunction extends RichFlatMapFunction<UserProductModel, UserProductModel> {

    private MapStateDescriptor<String, Boolean> deduplicateMapStateDescriptor = new MapStateDescriptor<String, Boolean>("deduplicate", String.class, Boolean.class);

    private transient MapState<String, Boolean> deduplicateMapState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.deduplicateMapState = this.getRuntimeContext().getMapState(deduplicateMapStateDescriptor);
    }

    @Override
    public void flatMap(UserProductModel userProductModel, Collector<UserProductModel> collector) throws Exception {
        Boolean isExists = this.deduplicateMapState.contains(userProductModel.getUserId());
        if (!isExists){
            this.deduplicateMapState.put(userProductModel.getUserId(), true);
            collector.collect(userProductModel);
        }
    }
}
