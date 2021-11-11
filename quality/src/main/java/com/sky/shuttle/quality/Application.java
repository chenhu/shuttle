package com.sky.shuttle.quality;


import com.sky.shuttle.quality.service.QualityService;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * 入口类
 */
@SpringBootApplication
public class Application {
    private static final String SERVICE = "qualityService";
    public static void main(String[] args) {
        try (ConfigurableApplicationContext context = new SpringApplicationBuilder().sources(Application.class).web(false).run(args)) {
            ((QualityService)context.getBean(SERVICE)).compute();
        }
    }
}
