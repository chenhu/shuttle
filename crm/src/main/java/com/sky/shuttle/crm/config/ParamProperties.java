package com.sky.shuttle.crm.config;

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
    private String crmFilePath;
    private String crmSavePath;
    private int outPutPartitions;
}
