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
        if(args.length!=2){
            System.out.println("use command as：");
            System.out.println("flink run -d -t yarn-per-job -c com.xxx.xxx.Main xxxxxxxxxxx.jar application.properties tagsconfig.properties");
            return;
        }
        String appPropertiesPath = args[0];
        String tagsPropertiesPath = args[1];
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //数据湖数据源
        DataStreamSource<List<DataLakeTagMessage>> stream = env.addSource(new DataLakeSource(appPropertiesPath));
        SingleOutputStreamOperator<String> dataLakeMap = stream.map(new DataLakeMapFunction(tagsPropertiesPath));
        dataLakeMap.print();

        //Redis数据源
        DataStreamSource<List<RedisTimeseriesTagMessage>> listDataStreamSource = env.addSource(new RedisTimeseriesSource(appPropertiesPath));
        SingleOutputStreamOperator<String> redisMap = listDataStreamSource.map(new TimeSeriesMapFunction(tagsPropertiesPath));
        redisMap.print();
        env.execute();

    }

}
