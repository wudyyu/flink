package com.flink.wudy.clickhouse.transfer;

import com.baomidou.mybatisplus.core.MybatisConfiguration;
import com.clickhouse.jdbc.ClickHouseDataSource;
import com.flink.wudy.clickhouse.model.BillEntityModel;
import com.flink.wudy.config.MyBatisPlusConfig;
import com.flink.wudy.convetor.BillConvetor;
import com.flink.wudy.entity.BillEntity;
import com.flink.wudy.mapper.BillMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

//@Slf4j
public class ClickHouseSinkFunction extends RichSinkFunction<BillEntityModel> {
    private transient BillMapper billMapper;

    private transient SqlSessionFactory sqlSessionFactory;

    private List<BillEntity> batchList = new ArrayList<>();
    private static final int BATCH_SIZE = 1000;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 手动注册Mapper接口
        MybatisConfiguration configuration = new MybatisConfiguration();
        configuration.addMapper(BillMapper.class);
        configuration.setMapUnderscoreToCamelCase(true);

        // 创建数据源
        String url = "jdbc:clickhouse://localhost:8123/wudy";
        Properties properties = new Properties();
        properties.setProperty("user", "default");
        properties.setProperty("password", "WOaiyuyu123");
        DataSource dataSource = new ClickHouseDataSource(url, properties);

        sqlSessionFactory = MyBatisPlusConfig.createSqlSessionFactory(dataSource);
        billMapper = sqlSessionFactory.openSession().getMapper(BillMapper.class);
        batchList = new ArrayList<>(BATCH_SIZE);
    }

    @Override
    public void invoke(BillEntityModel billEntityModel, Context context) throws Exception {
        try {
            // 将数据转换为适合 ClickHouse 的格式并插入
            BillEntity billEntity = BillConvetor.INSTANCE.toBillEntity((billEntityModel));
            batchList.add(billEntity);

            if (batchList.size() >= BATCH_SIZE){
                executeBatchInsert();
                batchList.clear();
            }
        } catch (Exception e) {
            // 处理异常，例如重试或记录错误日志
            System.out.println("Error inserting data" + e.getLocalizedMessage());
//            log.error("Error inserting data: {}", e.getMessage());
        }
    }

    @Override
    public void close() throws Exception {
        if (!batchList.isEmpty()) {
            executeBatchInsert();
        }
        super.close();
    }

    private void executeBatchInsert() {
        try (SqlSession sqlSession = sqlSessionFactory.openSession()) {
            BillMapper mapper = sqlSession.getMapper(BillMapper.class);
            for (BillEntity entity : batchList) {
                mapper.insert(entity);
            }
            sqlSession.commit();
        } catch (Exception e) {
            System.err.println("Batch insert failed: " + e.getMessage());
        }
    }

}
