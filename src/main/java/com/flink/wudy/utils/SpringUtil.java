package com.flink.wudy.utils;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ApplicationObjectSupport;
import org.springframework.stereotype.Component;
import javax.annotation.PostConstruct;

@Component
public class SpringUtil extends ApplicationObjectSupport {
    private static ApplicationContext applicationContext = null;

    public static <T>T getBean(String beanName){
        return (T) applicationContext.getBean(beanName);
    }

    @PostConstruct
    public void init(){
        applicationContext = super.getApplicationContext();
    }
}
