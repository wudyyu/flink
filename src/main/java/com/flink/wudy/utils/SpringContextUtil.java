package com.flink.wudy.utils;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.util.Map;

@Lazy
@Component
public class SpringContextUtil implements ApplicationContextAware {
    private static ApplicationContext applicationContext = null;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        if (SpringContextUtil.applicationContext == null) {
            this.applicationContext = applicationContext;
        }
    }

    /**
     * 检查应用上下文是否已初始化
     */
    private static void checkApplicationContext() {
        if (applicationContext == null) {
            throw new IllegalStateException("ApplicationContext未注入，请在Spring配置文件中定义SpringContextUtil");
        }
    }

    /**
     * 通过名称获取bean
     */
    public static <T>T getBeanByName(String beanName){
        checkApplicationContext();
        return (T) applicationContext.getBean(beanName);
    }

    /**
     * 通过类型获取bean
     */
    public static <T>T getBeanByType(Class<T> clazz){
        checkApplicationContext();
        Map<String, T> beanMap = applicationContext.getBeansOfType(clazz);
        if (beanMap.isEmpty()) {
            throw new IllegalArgumentException("未找到类型为 " + clazz.getName() + " 的Bean");
        }
        // 返回第一个匹配的Bean
        return beanMap.values().iterator().next();
    }
}
