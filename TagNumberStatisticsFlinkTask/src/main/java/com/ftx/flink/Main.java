package com.ftx.flink;

import com.ftx.flink.model.TagMessage;
import com.ftx.flink.sourceFunction.DataLakeSource;
import com.ftx.flink.utils.ConfigUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.Map;

public class Main {
    final static Logger logger = LoggerFactory.getLogger(Main.class);
    public static void main(String[] args) {
        String appPropertiesPath = args[0];
        String tagsPropertiesPath = args[1];
        if(StringUtils.isAnyBlank(appPropertiesPath,tagsPropertiesPath)){
            System.out.println("use command as：");
            System.out.println("flink run -d -t yarn-per-job -c com.xxx.xxx.Main xxxxxxxxxxx.jar application.properties tagsconfig.properties");
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //加载数据源
        DataStreamSource<List<TagMessage>> stream = env.addSource(new DataLakeSource(appPropertiesPath));
        SingleOutputStreamOperator<Object> map = stream.map(new MapFunction<List<TagMessage>, Object>() {
            @Override
            public Object map(List<TagMessage> tagMessages) throws Exception {
                if(tagMessages == null || tagMessages.isEmpty()){
                    logger.error("datasource is empty");
                    return null;
                }
                String tagNo = tagMessages.get(0).getTagNo();
                Date ts = tagMessages.get(0).getTs();
                long time = ts.getTime();
                Map<String, String> stringMap = ConfigUtil.readConfig_properties(tagsPropertiesPath);
                String rule = stringMap.get(tagNo);
                long timestamp = solveRule(rule);


                return null;
            }
        });


    }

    private static long solveRule(String rule) {
        String tag = rule.substring(rule.length() - 1);
        switch (tag){
            case "s":
                break;
            case "m":
                break;
            case "h":
                break;
            default:
                logger.error("位号时间间隔的规则配置有误，不支持{}，当前仅支持s/m/h",tag);
                break;
        }
        return 0;
    }
}
