package com.ftx.flink;

import com.ftx.flink.model.DataLakeTagMessage;
import com.ftx.flink.model.RedisTimeseriesTagMessage;
import com.ftx.flink.operator.DataLakeMapFunction;
import com.ftx.flink.operator.TimeSeriesMapFunction;
import com.ftx.flink.sourceFunction.DataLakeSource;
import com.ftx.flink.sourceFunction.RedisTimeseriesSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.*;

public class Main {
    public static void main(String[] args) throws Exception{
//        if(args.length!=2){
//            System.out.println("use command as：");
//            System.out.println("flink run -d -t yarn-per-job -c com.xxx.xxx.Main xxxxxxxxxxx.jar application.properties tagsconfig.properties");
//            return;
//        }
        //全局配置文件
//        String appPropertiesPath = args[0];
        String appPropertiesPath = "D:\\Code\\tag-number-statistics-flink-task\\TagNumberStatisticsFlinkTask\\src\\main\\resources\\application.properties";
        //位号时间间隔配置文件
//        String tagsPropertiesPath = args[1];
        String tagsPropertiesPath = "D:\\Code\\tag-number-statistics-flink-task\\TagNumberStatisticsFlinkTask\\src\\main\\resources\\tagsconfig.properties";
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //加载数据源
//        DataStreamSource<List<DataLakeTagMessage>> stream = env.addSource(new DataLakeSource(appPropertiesPath));
        //转换操作，进行统计
//        SingleOutputStreamOperator<String> map = stream.map(new DataLakeMapFunction(tagsPropertiesPath));
        //执行批处理的任务反馈打印出来
//        map.print();

        DataStreamSource<List<RedisTimeseriesTagMessage>> listDataStreamSource = env.addSource(new RedisTimeseriesSource(appPropertiesPath));
        SingleOutputStreamOperator<String> map = listDataStreamSource.map(new TimeSeriesMapFunction(tagsPropertiesPath));
        map.print();
        env.execute();

    }

}
