package com.sky.shuttle.quality.service;

import com.google.common.collect.Lists;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * author: ChenHu <chenhu1008@me.com>
 * date: 2019/4/6 16:58
 * description: 信令相关的Schema定义
 */
public class SignalSchemaProvider {
    // 原始数据分日期
    public static final StructType SIGNAL_SCHEMA_ORGINAL = DataTypes.createStructType(Lists.newArrayList(
            DataTypes.createStructField("date", DataTypes.IntegerType, false),
            DataTypes.createStructField("line", DataTypes.StringType, false)));
    //原始数据合并了基站后的数据
    public static final StructType SIGNAL_SCHEMA_BASE = DataTypes.createStructType(Lists.newArrayList(
            DataTypes.createStructField("date", DataTypes.IntegerType, false),
            DataTypes.createStructField("msisdn", DataTypes.StringType, false),
            DataTypes.createStructField("region", DataTypes.IntegerType, false),//号码归属地,本省到市，外省到省
            DataTypes.createStructField("city_code", DataTypes.IntegerType, false),//上报地市
            DataTypes.createStructField("district_code", DataTypes.IntegerType, false),//上报区县编码
            DataTypes.createStructField("tac", DataTypes.IntegerType, false),
            DataTypes.createStructField("cell", DataTypes.LongType, false),
            DataTypes.createStructField("base", DataTypes.StringType, false),
            DataTypes.createStructField("lng", DataTypes.DoubleType, false),
            DataTypes.createStructField("lat", DataTypes.DoubleType, false),
            DataTypes.createStructField("begin_time", DataTypes.TimestampType, false),
            DataTypes.createStructField("last_time", DataTypes.TimestampType, false)));
    public static final StructType SIGNAL_SCHEMA_BASE_1 = DataTypes.createStructType(Lists.newArrayList(
            DataTypes.createStructField("date", DataTypes.IntegerType, false),
            DataTypes.createStructField("msisdn", DataTypes.StringType, false),
            DataTypes.createStructField("region", DataTypes.IntegerType, false),
            DataTypes.createStructField("city_code", DataTypes.IntegerType, false),
            DataTypes.createStructField("district_code", DataTypes.IntegerType, false),//上报区县编码
            DataTypes.createStructField("tac", DataTypes.IntegerType, false),
            DataTypes.createStructField("cell", DataTypes.LongType, false),
            DataTypes.createStructField("base", DataTypes.StringType, false),
            DataTypes.createStructField("lng", DataTypes.DoubleType, false),
            DataTypes.createStructField("lat", DataTypes.DoubleType, false),
            DataTypes.createStructField("begin_time", DataTypes.TimestampType, false),
            DataTypes.createStructField("last_time", DataTypes.TimestampType, false),
            DataTypes.createStructField("distance", DataTypes.IntegerType, false),
            DataTypes.createStructField("move_time", DataTypes.IntegerType, false),
            DataTypes.createStructField("speed", DataTypes.DoubleType, false)));
    public static final StructType SIGNAL_SCHEMA_NO_AREA = DataTypes.createStructType(Lists.newArrayList(
            DataTypes.createStructField("date", DataTypes.IntegerType, false),
            DataTypes.createStructField("msisdn", DataTypes.StringType, false),
            DataTypes.createStructField("region", DataTypes.IntegerType, false),
            DataTypes.createStructField("city_code", DataTypes.IntegerType, false),
            DataTypes.createStructField("district_code", DataTypes.IntegerType, false),//区县编码
            DataTypes.createStructField("cen_region", DataTypes.IntegerType, false),// 户籍所在地
            DataTypes.createStructField("sex", DataTypes.ShortType, false),
            DataTypes.createStructField("age", DataTypes.ShortType, false),
            DataTypes.createStructField("tac", DataTypes.IntegerType, false),
            DataTypes.createStructField("cell", DataTypes.LongType, false),
            DataTypes.createStructField("base", DataTypes.StringType, false),
            DataTypes.createStructField("lng", DataTypes.DoubleType, false),
            DataTypes.createStructField("lat", DataTypes.DoubleType, false),
            DataTypes.createStructField("begin_time", DataTypes.TimestampType, false),
            DataTypes.createStructField("last_time", DataTypes.TimestampType, false),
            DataTypes.createStructField("distance", DataTypes.IntegerType, false),
            DataTypes.createStructField("move_time", DataTypes.IntegerType, false),
            DataTypes.createStructField("speed", DataTypes.DoubleType, false)));
}
