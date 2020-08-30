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
 *  统计平台用户数据的增长，主要就是注册量和订单量。我们定义几个要展示的统计指标：（只统计展示近七天的数据）
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
        // %s：占位符
        // 总结：group by是在select之前执行；而order by是在select之后执行。
        String memberSql = "select date_format(create_time,'yyyy-MM-dd') as day," +
                " count(id) as regCount, max(id) as memberCount " +
                " from ecommerce.t_member where create_time >='%s' " +
                " group by date_format(create_time,'yyyy-MM-dd') order by day";

        /**
         * sql可以直接执行
         * select date_format(create_time,'yyyy-MM-dd') as day,count(id) as regCount, max(id) as memberCount from ecommerce.t_member where create_time >='%s' group by date_format(create_time,'yyyy-MM-dd') order by day;
         */

        memberSql = String.format(memberSql, DateUtil.DateToString(sevenDayBefore, DateStyle.YYYY_MM_DD_HH_MM_SS)); //替换掉%s位置的内容
        Dataset<Row> memberDs = session.sql(memberSql);


        // 近七天订单和流水统计
        String orderSql = "select date_format(create_time,'yyyy-MM-dd') as day," +
                " max(order_id) orderCount, sum(origin_price) as gmv" +
                " from ecommerce.t_order where create_time >='%s' " +
                "group by date_format(create_time,'yyyy-MM-dd') order by day";
        orderSql = String.format(orderSql, DateUtil.DateToString(sevenDayBefore, DateStyle.YYYY_MM_DD_HH_MM_SS));
        Dataset<Row> orderDs = session.sql(orderSql);

        // 联接查询，按照day内连接
        // 内连接inner join
        Dataset<Tuple2<Row, Row>> tuple2Dataset = memberDs.joinWith(orderDs, memberDs.col("day").equalTo(orderDs.col("day")), "inner");


        // 先转换成list，然后遍历，取出每天数据，转换为vo，放入新的list
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

        /**
         * vos:
         *  size = 8
         *  GrowthEtl.GrowthLineVo(day=2019-11-23, regCount=50, memberCount=699, orderCount=559, gmv=41641.0)
         *  GrowthEtl.GrowthLineVo(day=2019-11-24, regCount=50, memberCount=749, orderCount=599, gmv=17266.0)
         *  GrowthEtl.GrowthLineVo(day=2019-11-25, regCount=50, memberCount=799, orderCount=639, gmv=57813.0)
         *  GrowthEtl.GrowthLineVo(day=2019-11-26, regCount=50, memberCount=849, orderCount=679, gmv=74616.0)
         *  GrowthEtl.GrowthLineVo(day=2019-11-27, regCount=50, memberCount=899, orderCount=719, gmv=48378.0)
         *  GrowthEtl.GrowthLineVo(day=2019-11-28, regCount=50, memberCount=949, orderCount=759, gmv=19381.0)
         *  GrowthEtl.GrowthLineVo(day=2019-11-29, regCount=50, memberCount=999, orderCount=799, gmv=63870.0)
         *  GrowthEtl.GrowthLineVo(day=2019-11-30, regCount=1, memberCount=1000, orderCount=800, gmv=114.0)
         */
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

        // 遍历总量gmv的List，更新vos里面gmv的值:理解
        for (int i = 0; i < totalGmvList.size(); i++) {
            GrowthLineVo lineVo = vos.get(i);
            lineVo.setGmv(totalGmvList.get(i));
        }

        return vos;
    }
    @Data
    static class GrowthLineVo {
        // 每天新增注册数、总用户数、总订单数、总流水GMV
        private String day; //当前日期
        private Integer regCount; //当天新增注册人数
        private Integer memberCount; //当天位置，平台总注册人数
        private Integer orderCount;  //当天为止，平台总订单数
        private BigDecimal gmv; //当天为止，平台订单交易总流水

    }
}