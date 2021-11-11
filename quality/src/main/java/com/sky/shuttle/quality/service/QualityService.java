package com.sky.shuttle.quality.service;

import com.sky.shuttle.apis.FileUtil;
import com.sky.shuttle.quality.config.ParamProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.util.Map;

import static org.apache.spark.sql.functions.*;

/**
 * author: ChenHu <chenhu1008@me.com>
 * date: 2019/5/13 17:22
 * description: 原始数据质量验证服务
 */
@Service
@Slf4j
public class QualityService {
    private final transient JavaSparkContext sparkContext;
    private final transient ParamProperties params;
    private final transient SignalLoader signalLoader;
    public QualityService(JavaSparkContext sparkContext, ParamProperties params, SignalLoader signalLoader) {
        this.sparkContext = sparkContext;
        this.params = params;
        this.signalLoader = signalLoader;
    }

    public void compute() {
        //删除临时文件
        this.clearQualityData();

        Map<String, Tuple2<String, String>> signalMap = params.getSignalFilePathTuple2();
        for(String date: signalMap.keySet()) {
            String traceFilePath = signalMap.get(date)._2;
            String validSignalFilePath = signalMap.get(date)._1;
            process(date,traceFilePath, validSignalFilePath);
        }
        DataFrame dataQualityAllStat = FileUtil.readFile(FileUtil.FileType.CSV, DataQualitySchemaProvider.SIGNAL_SCHEMA_BASE,
                params.getBasePath().concat(params.getQualityPartSavePath()).concat("*/")).orderBy("date");
        FileUtil.saveFile(dataQualityAllStat.repartition(1), FileUtil.FileType.CSV, params.getBasePath().concat(params.getQualityAllSavePath()));

    }
    private void process(String date, String traceFilePath, String validSignalFilePath) {
        SQLContext sc = new SQLContext(sparkContext);
        DataFrame orginalSignal =sc.read().parquet(traceFilePath).repartition(params.getInputPartitions());
        // 统计正常数据分布情况
        DataFrame validSignal = signalLoader.load(validSignalFilePath).repartition(params.getInputPartitions());
        DataFrame agg1 = validSignal.groupBy("date").agg(countDistinct("msisdn").as("msisdn_count"),
                count("msisdn").as("valid_row_count"),
                countDistinct("base").as("base_count"));
        DataFrame agg2 = orginalSignal.withColumn("date", date_format(col("start_time"),"yyyyMMdd")).groupBy("date").agg(count("*").as("orginal_count"));
        DataFrame joinedDf = agg1.join(agg2, agg1.col("date").equalTo(agg2.col("date")))
                .select(
                        agg1.col("date"),
                        agg2.col("orginal_count"),
                        agg1.col("valid_row_count"),
                        agg1.col("base_count"),
                        agg1.col("msisdn_count")
                );
        // 保存结果
        FileUtil.saveFile(joinedDf.repartition(1), FileUtil.FileType.CSV, params.getBasePath().concat(params.getQualityPartSavePath()).concat(date));
    }
    private void clearQualityData() {
        FileUtil.removeDfsDirectory(params.getBasePath().concat(params.getQualityPartSavePath()));
        FileUtil.removeDfsDirectory(params.getBasePath().concat(params.getQualityAllSavePath()));
    }
}
