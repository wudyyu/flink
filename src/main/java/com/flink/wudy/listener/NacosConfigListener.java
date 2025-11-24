package com.flink.wudy.listener;

import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.config.listener.Listener;
import com.alibaba.nacos.api.exception.NacosException;
import org.yaml.snakeyaml.Yaml;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class NacosConfigListener {
    private static volatile List<Long> productIds = Arrays.asList(10086L, 32578L, 44132L);
    private static ConfigService configService;

    public static void init(String serverAddress, String dataId, String group) throws NacosException {
        Properties properties = new Properties();
        properties.put("serverAddr", serverAddress);
        configService = NacosFactory.createConfigService(properties);

        // 获取初始配置
        String initialConfig = configService.getConfig(dataId, group, 3000);
        if (initialConfig != null && !initialConfig.trim().isEmpty()) {
            updateProductIds(initialConfig);
        }

        // 添加配置监听器
        configService.addListener(dataId, group, new Listener() {
            @Override
            public void receiveConfigInfo(String configInfo) {
                updateProductIds(configInfo);
            }

            @Override
            public Executor getExecutor() {
                return null;
            }
        });
    }

    private static void updateProductIds(String configInfo) {
        try {
            Yaml yaml = new Yaml();
            Map<String, Object> configMap = yaml.load(configInfo);

            if (configMap != null && configMap.containsKey("productIds")) {
                String productIdsStr = (String) configMap.get("productIds");
                String[] ids = productIdsStr.split(",");
                Long[] productIdArray = new Long[ids.length];

                for (int i = 0; i < ids.length; i++) {
                    productIdArray[i] = Long.parseLong(ids[i].trim());
                }

                productIds = Arrays.asList(productIdArray);
                System.out.println("Nacos配置更新成功: " + productIds);
            } else {
                System.err.println("Nacos配置格式错误，缺少productIds字段");
            }
        } catch (Exception e) {
            System.err.println("解析Nacos配置失败: " + configInfo + ", 错误: " + e.getMessage());
        }
    }

    public static List<Long> getProductIds() {
        return productIds;
    }
}
