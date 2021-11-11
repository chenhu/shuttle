package com.sky.shuttle.cell.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties("param")
public class ParamProperties {
    /**
     * spark应用名称
     */
    private String appName;
    /**
     * spark主机url
     */
    private String masterUrl;
    private String basePath;
    private String cellFilePath;
    private String cellSavePath;
    private int outPutPartitions;
    private String geohashSavePath;
}
