package com.ray.userprofile.etl;

import com.alibaba.fastjson.JSON;
import com.ray.userprofile.utils.DateStyle;
import com.ray.userprofile.utils.DateUtil;
import com.ray.userprofile.utils.SparkUtils;
import lombok.Data;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 所谓周环比，week on week，就是比较两周的数据，最近一周跟上周相比；
 * 而且为了图像更清楚，我们可以对两周的数据，再做一个同比显示：
 * 每一天都跟上周的这一天比（周一跟周一比，周二跟周二比），看增长多少。
 */
public class WowEtl {
    public static void main(String[] args) {

        SparkSession session = SparkUtils.initSession();

        // 查询近一周的reg和order的数量
        List<RegVo> regVos = regWeekCount(session);
        List<OrderVo> orderVos = orderWeekCount(session);

        System.out.println("======" + regVos);
        System.out.println("======" + orderVos);
    }

    public static List<RegVo> regWeekCount(SparkSession session) {

        LocalDate now = LocalDate.of(2019, Month.NOVEMBER, 30);
        Date nowDay = Date.from(now.atStartOfDay(ZoneId.systemDefault()).toInstant());

        Date lastTwoWeekFirstDay = DateUtil.addDay(nowDay, -14);

        String sql = "select date_format(create_time,'yyyy-MM-dd') as day," +
                " count(id) as regCount from ecommerce.t_member " +
                " where create_time >='%s' and create_time < '%s' " +
                " group by date_format(create_time,'yyyy-MM-dd')";
        sql = String.format(sql,
                DateUtil.DateToString(lastTwoWeekFirstDay, DateStyle.YYYY_MM_DD_HH_MM_SS),
                DateUtil.DateToString(nowDay, DateStyle.YYYY_MM_DD_HH_MM_SS));
        Dataset<Row> dataset = session.sql(sql);
        List<String> list = dataset.toJSON().collectAsList();
        List<RegVo> result = list.stream()
                .map(str -> JSON.parseObject(str, RegVo.class))
                .collect(Collectors.toList());
        return result;
    }

    public static List<OrderVo> orderWeekCount(SparkSession session) {
        LocalDate now = LocalDate.of(2019, Month.NOVEMBER, 30);
        Date nowDay = Date.from(now.atStartOfDay(ZoneId.systemDefault()).toInstant());
        Date lastTwoWeekFirstDay = DateUtil.addDay(nowDay, -14);

        String sql = "select date_format(create_time,'yyyy-MM-dd') as day," +
                " count(order_id) as orderCount from ecommerce.t_order " +
                " where create_time >='%s' and create_time < '%s' " +
                " group by date_format(create_time,'yyyy-MM-dd')";
        sql = String.format(sql,
                DateUtil.DateToString(lastTwoWeekFirstDay, DateStyle.YYYY_MM_DD_HH_MM_SS),
                DateUtil.DateToString(nowDay, DateStyle.YYYY_MM_DD_HH_MM_SS));
        Dataset<Row> dataset = session.sql(sql);
        List<String> list = dataset.toJSON().collectAsList();
        List<OrderVo> result = list.stream()
                .map(str -> JSON.parseObject(str, OrderVo.class))
                .collect(Collectors.toList());
        return result;
    }

    @Data
    static class RegVo {
        private String day;
        private Integer regCount;
    }

    @Data
    static class OrderVo {
        private String day;
        private Integer orderCount;
    }
}