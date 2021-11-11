package com.sky.shuttle.crm.service;

import com.google.common.base.Strings;
import com.sky.shuttle.apis.FileUtil;
import com.sky.shuttle.crm.config.ParamProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.springframework.stereotype.Service;

/**
 * crm信息预处理服务，注意是缩小提供的crm文件大小，去除重复的crm用户信息
 */
@Service("crmService")
@Slf4j
public class CrmService {
    private final transient JavaSparkContext sparkContext;

    private final transient SQLContext sqlContext;

    private final transient ParamProperties params;

    public CrmService(JavaSparkContext sparkContext, SQLContext sqlContext, ParamProperties params) {
        this.sparkContext = sparkContext;
        this.sqlContext = sqlContext;
        this.params = params;
    }
    public void compute() {
        JavaRDD<String> orignalRDD=sparkContext.textFile(params.getBasePath().concat(params.getCrmFilePath()));
        JavaRDD<Row> userRdd= orignalRDD.map(new Function<String,Row>() {
            @Override
            public Row call(String line) {
                String msisdn=null;
                Short sex=-2;
                Short age=-2;
                Integer id=-1;
                if (!Strings.isNullOrEmpty(line)) {
                    String[] props = line.split(",");
                    if(props.length>=5) {
                        try{
                            msisdn = props[0];
                            age = Short.valueOf(props[2]);
                            sex = Short.valueOf(props[1]);
                            id = Integer.valueOf(props[3]);
                        }catch (Exception e){
                            msisdn=null;
                            sex=-2;
                            age=-2;
                            id=-1;
                        }
                    }
                }
                return RowFactory.create(msisdn,sex,age,id);
            }
        });
        userRdd=userRdd.filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row row) throws Exception {
                String msisdn=row.getString(0);
                Short sex=row.getShort(1);
                Short age=row.getShort(2);
                Integer id=row.getInt(3);
                return !(msisdn == null | sex == -2 | age == -2 | id == -1);
            }
        });
        DataFrame userDf = sqlContext.createDataFrame(userRdd, CrmSchemaProvider.CRM_SCHEMA).dropDuplicates();
        FileUtil.saveFile(userDf.repartition(params.getOutPutPartitions()), FileUtil.FileType.PARQUET, params.getBasePath().concat(params.getCrmSavePath()));
    }
}