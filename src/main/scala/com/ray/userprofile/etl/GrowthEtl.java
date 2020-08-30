package com.ray.userprofile.etl;

import com.alibaba.fastjson.JSONObject;
import com.ray.userprofile.utils.DateStyle;
import com.ray.userprofile.utils.DateUtil;
import com.ray.userprofile.utils.SparkUtils;
import lombok.Data;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 *
 * 统计平台用户数据的增长，主要就是注册量和订单量。我们定义几个要展示的统计指标：（只统计展示近七天的数据）
 * 近七天的、每天新增注册人数（每天增量）；
 * 近七天的、截至每天的总用户人数（每天总量）；
 * 近七天的、截至每天的总订单数（每天总量）；
 * 近七天的、截至每天的总订单流水金额数量（每天总量）
 */
public class GrowthEtl {
    public static void main(String[] args) {
        SparkSession session = SparkUtils.initSession();
        List<GrowthLineVo> growthLineVo = growthEtl(session);
        System.out.println(growthLineVo);
    }

    private static List<GrowthLineVo> growthEtl(SparkSession session) {
        // 指定“当前日期”是2019.11.30，这是数据决定的
        LocalDate now = LocalDate.of(2019, Month.NOVEMBER, 30);
        Date nowDay = Date.from(now.atStartOfDay(ZoneId.systemDefault()).toInstant());
        Date sevenDayBefore = DateUtil.addDay(nowDay, -7);

        // 近七天注册人数统计
        String memberSql = "select date_format(create_time,'yyyy-MM-dd') as day," +
                " count(id) as regCount, max(id) as memberCount " +
                " from ecommerce.t_member where create_time >='%s' " +
                " group by date_format(create_time,'yyyy-MM-dd') order by day";
        /**
         * sql可以直接执行
         * select date_format(create_time,'yyyy-MM-dd') as day,count(id) as regCount, max(id) as memberCount from ecommerce.t_member where create_time >='%s' group by date_format(create_time,'yyyy-MM-dd') order by day;
         */


        memberSql = String.format(memberSql, DateUtil.DateToString(sevenDayBefore, DateStyle.YYYY_MM_DD_HH_MM_SS));
        Dataset<Row> memberDs = session.sql(memberSql);

        // 近七天订单和流水统计
        String orderSql = "select date_format(create_time,'yyyy-MM-dd') as day," +
                " max(order_id) orderCount, sum(origin_price) as gmv" +
                " from ecommerce.t_order where create_time >='%s' " +
                "group by date_format(create_time,'yyyy-MM-dd') order by day";
        orderSql = String.format(orderSql, DateUtil.DateToString(sevenDayBefore, DateStyle.YYYY_MM_DD_HH_MM_SS));
        Dataset<Row> orderDs = session.sql(orderSql);

        // 联接查询，按照day内连接
        Dataset<Tuple2<Row, Row>> tuple2Dataset = memberDs.joinWith(orderDs, memberDs.col("day").equalTo(orderDs.col("day")), "inner");

        List<Tuple2<Row, Row>> tuple2s = tuple2Dataset.collectAsList();
        List<GrowthLineVo> vos = new ArrayList<>();

        // 遍历二元组List，包装 GrowthLineVo
        for (Tuple2<Row, Row> tuple2 : tuple2s) {
            Row row1 = tuple2._1();    // memberSql结果
            Row row2 = tuple2._2();    // orderSql结果

            JSONObject obj = new JSONObject();

            StructType schema = row1.schema();
            String[] strings = schema.fieldNames();
            for (String string : strings) {
                Object as = row1.getAs(string);
                obj.put(string, as);
            }

            schema = row2.schema();
            strings = schema.fieldNames();
            for (String string : strings) {
                Object as = row2.getAs(string);
                obj.put(string, as); 
            }

            GrowthLineVo growthLineVo = obj.toJavaObject(GrowthLineVo.class);
            vos.add(growthLineVo);
        }

        // 七天前，再之前的订单流水总和（GMV）
        String preGmvSql = "select sum(origin_price) as totalGmv from ecommerce.t_order where create_time <'%s'";
        preGmvSql = String.format(preGmvSql, DateUtil.DateToString(sevenDayBefore, DateStyle.YYYY_MM_DD_HH_MM_SS));
        Dataset<Row> gmvDs = session.sql(preGmvSql);

        double previousGmv = gmvDs.collectAsList().get(0).getDouble(0);
        BigDecimal preGmv = BigDecimal.valueOf(previousGmv);


        // 之前每天的增量gmv取出，依次叠加，得到总和
        List<BigDecimal> totalGmvList = new ArrayList<>();

        for (int i = 0; i < vos.size(); i++) {
            GrowthLineVo growthLineVo = vos.get(i);
            BigDecimal gmv = growthLineVo.getGmv();

            BigDecimal temp = gmv.add(preGmv);

            for (int j = 0; j < i; j++) {
                GrowthLineVo prev = vos.get(j);
                temp = temp.add(prev.getGmv());
            }

            totalGmvList.add(temp);
        }

        // 遍历总量gmv的List，更新vos里面gmv的值
        for (int i = 0; i < totalGmvList.size(); i++) {
            GrowthLineVo lineVo = vos.get(i);
            lineVo.setGmv(totalGmvList.get(i));
        }

        return vos;
    }
    @Data
    static class GrowthLineVo {
        // 每天新增注册数、总用户数、总订单数、总流水GMV
        private String day;
        private Integer regCount;
        private Integer memberCount;
        private Integer orderCount;
        private BigDecimal gmv;
    }
}