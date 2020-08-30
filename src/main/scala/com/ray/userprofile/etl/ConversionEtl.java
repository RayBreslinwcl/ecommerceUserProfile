package com.ray.userprofile.etl;


import com.ray.userprofile.utils.SparkUtils;
import lombok.Data;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 在一个电商平台上，用户可以有不同的业务行为：
 * 页面展现 -> 点击 -> 加购物车 -> 下单 -> 复购 -> 充值（购买优惠券）
 * 对于用户的一些行为，我们是希望做沉降分析的，考察用户每一步行为的转化率。这就可以画出一个“漏斗图”：
 */
public class ConversionEtl {
    public static void main(String[] args) {
        SparkSession session = SparkUtils.initSession();

        ConversionVo conversionVo = conversionBehaviorCount(session);
        System.out.println(conversionVo);
        //结果：
        //ConversionEtl.ConversionVo(present=1000, click=800, addCart=600, order=541, orderAgain=197, chargeCoupon=186)
    }

    public static ConversionVo conversionBehaviorCount(SparkSession session){
        // 查询下过订单的用户
        // order_status=2已完成，=1进行中，=3已取消
        Dataset<Row> orderMember = session.sql("select distinct(member_id) from ecommerce.t_order " +
                "where order_status=2");


        // 将购买次数超过 1 次的用户查出来
        Dataset<Row> orderAgainMember = session.sql("select t.member_id as member_id " +
                " from (select count(order_id) as orderCount," +
                " member_id from ecommerce.t_order " +
                " where order_status=2 group by member_id) as t " +
                " where t.orderCount>1");

        // 查询充值过的用户
        // coupon_channel：1 用户购买 2 公司发放
        Dataset<Row> charge = session.sql("select distinct(member_id) as member_id " +
                "from ecommerce.t_coupon_member where coupon_channel = 1");
        // join的原因：因为储值购买优惠卷的用户不一定是复购的用户，所以这个应该和上一步的复购用户取交集
        Dataset<Row> join = charge.join(
                orderAgainMember,
                orderAgainMember.col("member_id")
                        .equalTo(charge.col("member_id")),
                "inner");

        // 统计各层级的数量
        long order = orderMember.count();
        long orderAgain = orderAgainMember.count();
        long chargeCoupon = join.count();

        // 包装成VO
        ConversionVo vo = new ConversionVo();
        vo.setPresent(1000L); // 目前数据中没有，直接给定值
        vo.setClick(800L);
        vo.setAddCart(600L);
        vo.setOrder(order);
        vo.setOrderAgain(orderAgain);
        vo.setChargeCoupon(chargeCoupon);

        return vo;
    }

    @Data
    static class ConversionVo{
        //页面展现 -> 点击 -> 加购物车 -> 下单 -> 复购 -> 充值（购买优惠券）
        private Long present;  //浏览行为
        private Long click;  //点击行为（查看详情）
        private Long addCart;  //加购物车
        private Long order;  //下单购买
        private Long orderAgain; //复购行为
        private Long chargeCoupon; //充值（购买优惠券）
    }
}