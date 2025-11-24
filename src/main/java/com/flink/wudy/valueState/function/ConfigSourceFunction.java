package com.flink.wudy.valueState.function;

import com.flink.wudy.listener.NacosConfigListener;
import com.flink.wudy.valueState.model.ConfigModel;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Arrays;
import java.util.List;

/**
 * Nacos安装:
 * docker pull nacos/nacos-server
 *
 * 运行nacos:
 * docker run --env MODE=standalone --name nacos-standalone -d -p 8848:8848 nacos/nacos-server

   docker run --ulimit nofile=65536:65536  --name nacos --network=host \
    -e MODE=standalone \
    -e NACOS_SERVER_PORT=8848 \
     -e PREFER_HOST_MODE=hostname \
     -e SPRING_DATASOURCE_PLATFORM=mysql \
     -e MYSQL_SERVICE_HOST=127.0.0.1\
     -e MYSQL_SERVICE_PORT=3306 \
     -e MYSQL_SERVICE_DB_NAME=nacos \
     -e MYSQL_SERVICE_USER=root \
     -e MYSQL_SERVICE_PASSWORD=WOaiyuyu123 \
     -e TIME_ZONE='Asia/Shanghai' \
     -p 8848:8848 \
     -d nacos/nacos-server

 *
 *
 *
 * */
public class ConfigSourceFunction extends RichSourceFunction<ConfigModel> {
    private volatile boolean isCancel = false;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 初始化Nacos配置监听
        NacosConfigListener.init("127.0.0.1:8848", "product-config", "DEFAULT_GROUP");
    }

    @Override
    public void run(SourceContext<ConfigModel> sourceContext) throws Exception {
        while(!this.isCancel){
            List<Long> currentProductIds = NacosConfigListener.getProductIds();
            sourceContext.collect(
                    ConfigModel.builder()
                            // 模拟从配置中心nacos获取productIds
//                            .productIds(Arrays.asList(10086L, 32578L, 44132L))

                            // 从真实Nacos中获取productIds
                            .productIds(currentProductIds)
                            .build()
            );
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        this.isCancel = true;
    }
}
