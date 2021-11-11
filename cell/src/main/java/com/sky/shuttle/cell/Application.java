package com.sky.shuttle.cell;

import com.sky.shuttle.cell.service.CellService;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * 入口类
 */
@SpringBootApplication
public class Application {
    private static final String SERVICE = "cellService";
    public static void main(String[] args) {
        try (ConfigurableApplicationContext context = new SpringApplicationBuilder().sources(Application.class).web(false).run(args)) {
            ((CellService)context.getBean(SERVICE)).compute();
        }
    }
}
