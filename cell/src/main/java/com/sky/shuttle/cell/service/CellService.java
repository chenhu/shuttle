package com.sky.shuttle.cell.service;

import com.google.common.base.Strings;
import com.google.common.collect.Ordering;
import com.sky.shuttle.cell.config.ParamProperties;
import com.sky.shuttle.apis.FileUtil;
import com.sky.shuttle.apis.GeoUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.col;

/**
 * 基站数据处理服务
 */
@Service
@Slf4j
public class CellService  {
    private final transient JavaSparkContext sparkContext;

    private final transient SQLContext sqlContext;

    private final transient ParamProperties params;

    public CellService(JavaSparkContext sparkContext, SQLContext sqlContext, ParamProperties params) {
        this.sparkContext = sparkContext;
        this.sqlContext = sqlContext;
        this.params = params;
    }

    public void compute() {
        JavaRDD<String> lines = sparkContext.textFile(params.getBasePath().concat(params.getCellFilePath()));
        JavaRDD<Row> rdd = lines.map(new Function<String, Row>() {
            @Override
            public Row call(String line) throws Exception {
                Integer cityCode = 0;
                Integer districtCode = 0;
                Integer tac = 0;
                Long cell = 0L;
                Double lng = 0d;
                Double lat = 0d;
                String geoHash = null;
                if (!Strings.isNullOrEmpty(line)) {
                    String[] props = line.split(",");
                    if (props.length >= 4) {
                        try {
                            cityCode = Integer.valueOf(props[0]);
                            districtCode = Integer.valueOf(props[1]);
                            tac = Integer.valueOf(props[2]);
                            cell = Long.valueOf(props[3]);
                            lng = Double.valueOf(props[4]);
                            lat = Double.valueOf(props[5]);
                            geoHash = GeoUtil.geo(lat, lng);
                        } catch (Exception e) {
                            cityCode = 0;
                            tac = 0;
                            cell = 0L;
                            lng = 0d;
                            lat = 0d;
                            geoHash = "";
                        }
                    }
                }
                return RowFactory.create(cityCode, districtCode, tac, cell, lng, lat,
                        geoHash);
            }
        });

        DataFrame df = sqlContext.createDataFrame(rdd, CellSchemaProvider
                .CELL_SCHEMA_OLD);
        df = df.filter(col("tac").notEqual(0).and(col("cell").notEqual(0)));

        //对数据按照cell字段作升序排序，这样能保证取到最小的cell值为基站编码一部分
        Ordering<Row> ordering = Ordering.natural().nullsFirst().onResultOf
                (new com.google.common.base.Function<Row, Long>() {
                    @Override
                    public Long apply(Row row) {
                        return (Long) row.getAs("cell");
                    }
                });
        List<Row> rows = ordering.sortedCopy(df.collectAsList());

        //位置相同的基站使用同一个基站编号base
        List<Row> newRows = new ArrayList<>();
        Map<String, String> cellMap = new HashMap<>();
        for (Row row : rows) {
            String base = null;
            String position = String.format("%.6f|%.6f", row.getDouble(4),
                    row.getDouble(5));
            if (cellMap.containsKey(position)) {
                base = cellMap.get(position);
            } else {
                base = row.getAs("cell").toString();
                cellMap.put(position, base);
            }
            base = row.getAs("tac").toString() + '|' + base;
            newRows.add(RowFactory.create(
                    row.getAs("city_code"),
                    row.getAs("district_code"),
                    row.getAs("tac"),
                    row.getAs("cell"),
                    base,
                    row.getAs("lng"),
                    row.getAs("lat"),
                    row.getAs("geohash")
            ));
        }

        df = sqlContext.createDataFrame(newRows, CellSchemaProvider.CELL_SCHEMA).cache();
        FileUtil.saveFile(df.repartition(params.getOutPutPartitions()), FileUtil.FileType.CSV, params.getBasePath().concat(params.getCellSavePath()));
        // 生成geohash和经纬度对应表
        FileUtil.saveFile(df.select("lng", "lat", "geohash").dropDuplicates()
                .repartition(1), FileUtil.FileType.CSV, params.getBasePath().concat(params.getGeohashSavePath()));
    }
}