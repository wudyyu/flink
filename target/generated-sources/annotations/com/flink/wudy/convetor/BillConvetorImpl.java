package com.flink.wudy.convetor;

import com.flink.wudy.clickhouse.model.BillEntityModel;
import com.flink.wudy.entity.BillEntity;
import javax.annotation.processing.Generated;

@Generated(
    value = "org.mapstruct.ap.MappingProcessor",
    date = "2025-11-19T10:03:54+0800",
    comments = "version: 1.5.5.Final, compiler: javac, environment: Java 17.0.3.1 (Oracle Corporation)"
)
public class BillConvetorImpl implements BillConvetor {

    @Override
    public BillEntity toBillEntity(BillEntityModel billEntityModel) {
        if ( billEntityModel == null ) {
            return null;
        }

        BillEntity billEntity = new BillEntity();

        billEntity.setBillId( billEntityModel.getBillId() );
        billEntity.setProjectId( billEntityModel.getProjectId() );
        billEntity.setPrice( billEntityModel.getPrice() );
        billEntity.setRegionId( billEntityModel.getRegionId() );
        billEntity.setItemId( billEntityModel.getItemId() );
        billEntity.setChargeType( billEntityModel.getChargeType() );
        billEntity.setCreatedAt( billEntityModel.getCreatedAt() );
        billEntity.setStartTime( billEntityModel.getStartTime() );
        billEntity.setEndTime( billEntityModel.getEndTime() );

        return billEntity;
    }
}
