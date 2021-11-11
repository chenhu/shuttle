package com.sky.shuttle.crm;


import com.sky.shuttle.crm.service.CrmService;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * 入口类
 */
@SpringBootApplication
public class Application {
    private static final String SERVICE = "crmService";
    public static void main(String[] args) {
        try (ConfigurableApplicationContext context = new SpringApplicationBuilder().sources(Application.class).web(false).run(args)) {
            ((CrmService)context.getBean(SERVICE)).compute();
        }
    }
}
