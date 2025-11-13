package com.flink.wudy.clickhouse.source;

import com.flink.wudy.clickhouse.model.BillEntityModel;
import com.flink.wudy.clickhouse.model.ChargeType;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.springframework.util.CollectionUtils;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.*;

/**
 * 自定义10分钟账单数据源
 * 验证点:
 * 1.模拟在一个10分钟时间窗口，出现数据延迟的情况(前9条都在窗口内，第10条在窗口外)
 * 2.聚合相同数据中心、相同计费项 、同一个MSP 条件下，总账单金额，并生成一条新的小时账单写入CK
 * 3.
 * >
 */
public class BillEntitySource extends RichSourceFunction<BillEntityModel> {

    private static final Long ONE_MIN_SECONDS = 60L;

    private static final String ITEM_ID = "28771";
    private static final String REGION_ID = "10086";


    @Override
    public void run(SourceContext<BillEntityModel> sourceContext) throws Exception {
//        List<String> regionIds = Arrays.asList("10086", "11567");
//        List<String> itemIds = Arrays.asList("28771", "23456");
        List<Long> projectIds = Arrays.asList(32578L, 36778L);


        List<BillEntityModel> billEntityModels = new ArrayList<>();

        // 模拟生成10分钟的总账单
        for (Long projectId : projectIds){
            Long initTimeStamp = 1762617600L; // 2025-11-09 00:00:00
            billEntityModels.addAll(buildBillEntityModel(projectId, initTimeStamp));
        }

        if (!CollectionUtils.isEmpty(billEntityModels)){
            for (BillEntityModel billEntityModel: billEntityModels) {
                sourceContext.collect(billEntityModel);
            }
        }
    }

    private List<BillEntityModel> buildBillEntityModel(Long projectId, Long initTimeStamp){
        List<BillEntityModel> billEntityModelList = new ArrayList<>();
        for (int i=1 ; i<=10; i++){
            BillEntityModel billEntityModel = new BillEntityModel();
            billEntityModel.setProjectId(projectId);
            billEntityModel.setItemId(ITEM_ID);
            billEntityModel.setRegionId(REGION_ID);

            billEntityModel.setBillId(UUID.randomUUID().toString());
            int randomNumber = (new Random()).nextInt(10);
            billEntityModel.setPrice(new BigDecimal(randomNumber));
            billEntityModel.setChargeType(ChargeType.POST_PAID);
            Long startTime = initTimeStamp + (i - 1) * ONE_MIN_SECONDS;
            billEntityModel.setStartTime(startTime);
            Long endTime = startTime + ONE_MIN_SECONDS;
            billEntityModel.setEndTime(endTime);
            billEntityModel.setCreatedAt(LocalDateTime.now());

            billEntityModelList.add(billEntityModel);
        }
        return billEntityModelList;
    }

    @Override
    public void cancel() {

    }
}
