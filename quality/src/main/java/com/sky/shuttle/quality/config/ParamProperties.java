package com.sky.shuttle.quality.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

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
    private String validSignalSavePath;
    private String qualityPartSavePath;
    private String qualityAllSavePath;
    private String days;
    private String cityCodes;
    private int inputPartitions;
    private int outPutPartitions;

    public Map<String, Tuple2<String, String>> getSignalFilePathTuple2() {
        String[] days = this.getDays().split(",");
        String[] cities = this.getCityCodes().split(",");
        Map<String, Tuple2<String, String>> resultMap = new HashMap<>();
        for (String day : days) {
            for (String cityCode: cities) {
                resultMap.put(day, new Tuple2<>(getValidSignalSavePath(day,cityCode), getTraceFilesByDay(day,cityCode)));
            }
        }
        return resultMap;
    }
    private String getValidSignalSavePath(String date,String cityCode) {
        return this.getBasePath().concat("save")
                .concat(cityCode)
                .concat("/").concat(validSignalSavePath).concat("/")
                .concat(date);
    }

    private String getTraceFilesByDay(String day,String cityCode) {
        return this.getBasePath()
                .concat(cityCode)
                .concat("/trace/dt=")
                .concat(day);
    }
}
