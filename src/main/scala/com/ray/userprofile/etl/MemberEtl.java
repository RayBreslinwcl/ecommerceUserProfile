package com.ray.userprofile.etl;

import com.alibaba.fastjson.JSON;
import com.ray.userprofile.utils.SparkUtils;
import lombok.Data;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.stream.Collectors;

public class MemberEtl {
    public static void main(String[] args) {
        SparkSession session = SparkUtils.initSession();

        // 写sql查询数据
        List<MemberSex> memberSexes = memberSexEtl(session);
        List<MemberChannel> memberChannels = memberChannelEtl(session);
        List<MemberMpSub> memberMpSubs = memberMpSubEtl(session);
        MemberHeat memberHeat = memberHeatEtl(session);

        // 拼成需要的结果
        MemberVo memberVo = new MemberVo();
        memberVo.setMemberSexes(memberSexes);
        memberVo.setMemberChannels(memberChannels);
        memberVo.setMemberMpSubs(memberMpSubs);
        memberVo.setMemberHeat(memberHeat);

        //打印到控制台输出
        //这一步是输出到mysql或者redis，供前端查询
        System.out.println("===========" + JSON.toJSONString(memberVo));
        /**
         * 结果：
         * ===========
         * {"memberChannels":[{"channelCount":171,"memberChannel":1},{"channelCount":211,"memberChannel":3},{"channelCount":199,"memberChannel":5},{"channelCount":195,"memberChannel":4},{"channelCount":224,"memberChannel":2}],"memberHeat":{"complete":1000,"coupon":540,"order":344,"orderAgain":197,"reg":0},"memberMpSubs":[{"subCount":799,"unSubCount":201}],"memberSexes":[{"memberSex":-1,"sexCount":201},{"memberSex":1,"sexCount":397},{"memberSex":2,"sexCount":402}]}
         */

    }

    /**
     * 统计平台用户性别分布
     * @param session
     * @return
     */
    public static List<MemberSex> memberSexEtl(SparkSession session) {
        // 先用sql得到每个性别的count统计数据
        Dataset<Row> dataset = session.sql(
"select sex as memberSex, count(id) as sexCount " +
                " from ecommerce.t_member group by sex");

        List<String> list = dataset.toJSON().collectAsList();

        // 对每一个元素依次map成MemberSex，收集起来
        List<MemberSex> result = list.stream()
                .map( str -> JSON.parseObject(str, MemberSex.class))
                .collect(Collectors.toList());

        return result;
    }

    /**
     * 用户注册渠道
     * @param session
     * @return
     */
    public static List<MemberChannel> memberChannelEtl(SparkSession session) {
        Dataset<Row> dataset = session.sql(
                "select member_channel as memberChannel, count(id) as channelCount " +
                " from ecommerce.t_member group by member_channel");

        List<String> list = dataset.toJSON().collectAsList();

        List<MemberChannel> result = list.stream()
                .map(str -> JSON.parseObject(str, MemberChannel.class))
                .collect(Collectors.toList());

        return result;
    }

    /**
     * 用户是否关注媒体平台
     * @param session
     * @return
     */
    public static List<MemberMpSub> memberMpSubEtl(SparkSession session) {
        //解释：open_id为空，则没有注册
        //count里面加if标准判断用法，count(if(判断条件,true统计,null不统计))
        //理解count if用法，即可理解
        Dataset<Row> sub = session.sql(
                "select count(if(mp_open_id !='null',true,null)) as subCount, " +
                " count(if(mp_open_id ='null',true,null)) as unSubCount " +
                " from ecommerce.t_member");


        List<String> list = sub.toJSON().collectAsList();

        List<MemberMpSub> result = list.stream()
                .map(str -> JSON.parseObject(str, MemberMpSub.class))
                .collect(Collectors.toList());

        return result;
    }

    /**
     * 用户热度统计
     * @param session
     * @return
     */
    public static MemberHeat memberHeatEtl(SparkSession session) {
        // reg , complete , order , orderAgain, coupon
        // reg,complete从t_member中取出
        Dataset<Row> reg_complete = session.sql(
                "select count(if(phone='null',true,null)) as reg," +
                " count(if(phone !='null',true,null)) as complete " +
                " from ecommerce.t_member");
        //采用reg_complete.toJSON().collectAsList()判断，结果是{"reg":0,"complete":1000}

        // order和orderAgain：下过单和下过两次以上单的用户统计
        Dataset<Row> order_again = session.sql(
                "select count(if(t.orderCount =1,true,null)) as order," +
                "count(if(t.orderCount >=2,true,null)) as orderAgain from " +
                "(select count(order_id) as orderCount,member_id from ecommerce.t_order group by member_id) as t");
        //采用order_again.toJSON().collectAsList()判断，结果是{"order":344,"orderAgain":197}

        // coupon：购买过优惠卷的用户
        Dataset<Row> coupon = session.sql("select count(distinct member_id) as coupon from ecommerce.t_coupon_member ");
        //采用coupon.toJSON().collectAsList()判断，结果是{"coupon":540}

        // 最终，将三张表（注册、复购、优惠券）连在一起
        Dataset<Row> heat = coupon.crossJoin(reg_complete).crossJoin(order_again);
        // coupon.crossJoin(reg_complete).toJSON().collectAsList() 结果：{"coupon":540,"reg":0,"complete":1000}
        // coupon.crossJoin(reg_complete).crossJoin(order_again).toJSON().collectAsList() 结果：{"coupon":540,"reg":0,"complete":1000,"order":344,"orderAgain":197}


        List<String> list = heat.toJSON().collectAsList();
        //list转为stream，然后后续操作
        List<MemberHeat> result = list.stream()
                .map(str -> JSON.parseObject(str, MemberHeat.class))
                .collect(Collectors.toList());

        // 只有一行数据，获取后返回
        return result.get(0);
    }

    // 想要展示饼图的数据信息
    @Data
    static class MemberVo{
        private List<MemberSex> memberSexes;    // 性别统计信息
        private List<MemberChannel> memberChannels;  // 渠道来源统计信息
        private List<MemberMpSub> memberMpSubs;  // 用户是否关注媒体平台
        private MemberHeat memberHeat;   // 用户热度统计
    }
    // 分别定义每个元素类
    @Data
    static class MemberSex {
        private Integer memberSex; //性别编号
        private Integer sexCount;  //当前性别count数量
    }
    @Data
    static class MemberChannel {
        private Integer memberChannel; //渠道编号
        private Integer channelCount;
    }
    @Data
    static class MemberMpSub {
        private Integer subCount;
        private Integer unSubCount;
    }
    @Data
    static class MemberHeat {
        private Integer reg;    // 只注册，未填写手机号的用户统计数
        private Integer complete;    // 完善了信息，填了手机号的用户统计数
        private Integer order;    // 下过订单的用户统计数
        private Integer orderAgain;    // 多次下单，复购的用户统计数
        private Integer coupon;    // 购买过优惠券，储值的用户统计数
    }
}