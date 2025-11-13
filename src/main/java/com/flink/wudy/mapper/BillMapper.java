package com.flink.wudy.mapper;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.flink.wudy.entity.BillEntity;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import java.util.List;

@DS("clickhouse")
@Mapper
public interface BillMapper extends BaseMapper<BillEntity> {
    @Select("SELECT * FROM bill WHERE project_id = #{projectId}")
    List<BillEntity> selectBillListBy(String projectId);

    default void batchInsert(List<BillEntity> entities){
        for (BillEntity entity : entities){
            insert(entity);
        }
    }
}
