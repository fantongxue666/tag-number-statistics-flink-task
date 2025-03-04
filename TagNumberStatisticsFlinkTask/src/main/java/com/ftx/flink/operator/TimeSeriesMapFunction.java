package com.ftx.flink.operator;
import com.ftx.flink.Main;
import com.ftx.flink.model.RedisTimeseriesTagMessage;
import com.ftx.flink.utils.ConfigUtil;
import com.ftx.flink.utils.ConnectionUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.Connection;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 转换算子，统计时序数据库的每一天缺少的位号时刻数据，并记录
 */
public class TimeSeriesMapFunction  implements MapFunction<List<RedisTimeseriesTagMessage>, String> {
    final static Logger logger = LoggerFactory.getLogger(Main.class);
    String tagsPropertiesPath = StringUtils.EMPTY;
    String appPropertiesPath = StringUtils.EMPTY;

    public TimeSeriesMapFunction(String appPropertiesPath,String tagsPropertiesPath) {
        this.appPropertiesPath = appPropertiesPath;
        this.tagsPropertiesPath = tagsPropertiesPath;
    }
    @Override
    public String map(List<RedisTimeseriesTagMessage> values) throws Exception {
        if (values == null || values.isEmpty()) {
            logger.error("datasource is empty");
            return null;
        }
        List<Long> collect = values.stream().map(s -> s.getTimestamp()).collect(Collectors.toList());
        Collections.sort(collect, new Comparator<Long>() {
            @Override
            public int compare(Long o1, Long o2) {
                return o1 > o2 ? 1 : -1;
            }
        });
        String tagNo = values.get(0).getKey();
        Map<String, String> stringMap = ConfigUtil.readConfig_properties(tagsPropertiesPath);
        String rule = stringMap.get(tagNo);
        //时间间隔
        long timestamp = solveRule(rule);
        if(timestamp == 0){
            logger.error("请检查位号{}时间间隔配置，看是否已配置或配置错误",tagNo);
            return null;
        }

        Long minTimeStamp = collect.get(0);
        Long maxTimeStamp = collect.get(collect.size() - 1);
        String format = StringUtils.EMPTY;
        format = new SimpleDateFormat("yyyy-MM-dd").format(new Date(minTimeStamp));

        //统计
        //缺失数
        int count = 0;
        StringBuilder timestampsString = new StringBuilder();
        while (minTimeStamp < maxTimeStamp) {
            if (!collect.contains(minTimeStamp)) {
                //缺少数据，暂时打印进行验证
                logger.info("缺少数据！位号：{} 缺少日期：{} 缺失时刻：{}", tagNo, format, minTimeStamp);
                count++;
                timestampsString.append(String.valueOf(minTimeStamp)).append(",");
            }
            minTimeStamp += timestamp;
        }

        if(!StringUtils.isBlank(timestampsString)){
            String timestampsStringResult = timestampsString.toString().substring(0,timestampsString.length()-1);
            //存储
            String insertSql = "insert into DeletionDataStat values ('"+UUID.randomUUID().toString()+"','"+tagNo+"','"+format+"',"+count+",'"+timestampsStringResult+"')";
            System.out.println(insertSql);
            Connection connection = ConnectionUtil.getIcebergConnection(appPropertiesPath);
            Statement statement = connection.createStatement();
            boolean b = statement.execute(insertSql);
            if(!b)logger.error("ERROR! 位号"+tagNo + "检测结果插入iceberg失败");
        }

        String result = "位号" + tagNo + "，日期为" + format + "已检测完成";
        return result;
    }

    /**
     * 根据位号配置的时间间隔得到 间隔的时间戳大小
     *
     * @param rule
     * @return
     */
    private static long solveRule(String rule) {
        if(StringUtils.isBlank(rule)){
            return 0;
        }
        String tag = rule.substring(rule.length() - 1);
        String numStr = rule.substring(0, rule.length() - 1);
        long result = 0;
        switch (tag) {
            case "s":
                result = Integer.valueOf(numStr) * 1000;
                break;
            case "m":
                result = Integer.valueOf(numStr) * 60 * 1000;
                break;
            case "h":
                result = Integer.valueOf(numStr) * 60 * 60 * 1000;
                break;
            default:
                logger.error("位号时间间隔的规则配置有误，不支持{}，当前仅支持s/m/h", tag);
                break;
        }
        return result;
    }
}
