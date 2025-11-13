package com.flink.wudy.convetor;

import com.flink.wudy.clickhouse.model.BillEntityModel;
import com.flink.wudy.entity.BillEntity;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.factory.Mappers;

@Mapper
public interface BillConvetor {
    BillConvetor INSTANCE = Mappers.getMapper(BillConvetor.class);

    @Mapping(source = "billEntityModel.billId", target = "billId")
    @Mapping(source = "billEntityModel.projectId", target = "projectId")
    @Mapping(source = "billEntityModel.price", target = "price")
    @Mapping(source = "billEntityModel.regionId", target = "regionId")
    @Mapping(source = "billEntityModel.itemId", target = "itemId")
    @Mapping(source = "billEntityModel.chargeType", target = "chargeType")
    @Mapping(source = "billEntityModel.createdAt", target = "createdAt")
    @Mapping(source = "billEntityModel.startTime", target = "startTime")
    @Mapping(source = "billEntityModel.endTime", target = "endTime")
    BillEntity toBillEntity(BillEntityModel billEntityModel);
}
