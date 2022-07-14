package com.ftx.flink.sourceFunction;

import com.ftx.flink.model.RedisTimeseriesTagMessage;
import com.ftx.flink.utils.ConfigUtil;
import io.github.dengliming.redismodule.redistimeseries.RedisTimeSeries;
import io.github.dengliming.redismodule.redistimeseries.Sample;
import io.github.dengliming.redismodule.redistimeseries.client.RedisTimeSeriesClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.redisson.config.ClusterServersConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Client;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 时序redis的源数据
 */
public class RedisTimeseriesSource extends RichSourceFunction<List<RedisTimeseriesTagMessage>> {
    final Logger logger = LoggerFactory.getLogger(DataLakeSource.class);

    //application.properties文件的绝对路径
    private String appPropertiesPath = StringUtils.EMPTY;
    //建立的redis连接
    private RedisTimeSeriesClient rtsc = null;
    //状态，标记是否第一次执行
    ValueState<Boolean> state = null;

    /**
     * 初始化
     * @param appPropertiesPath
     */
    public RedisTimeseriesSource(String appPropertiesPath) {
        this.appPropertiesPath = appPropertiesPath;
        //初始化状态为false，表示未执行
        ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>("redisSourceState", Types.BOOLEAN);
        state = getRuntimeContext().getState(descriptor);
        try {
            state.update(false);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run(SourceContext<List<RedisTimeseriesTagMessage>> ctx) throws Exception {
        //初始化连接
        this.rtsc = initConnect();
        RedisTimeSeries rts = rtsc.getRedisTimeSeries();
        if (null == rts) {
            logger.error("Connection to data source of RedisTimeseries failed");
        }
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Map<String, String> configMap = ConfigUtil.readConfig_properties(appPropertiesPath);
        String hostsStr = configMap.get("redis.hosts");
        String[] redisHosts = hostsStr.split(",");
        //得到所有的位号
        List<String> ckeys = getRedisClusterKeys(redisHosts);
        if(!state.value()){
            logger.info("程序第一次执行，处理时序redis的所有数据");
            //开始时间
            String startDateStr = configMap.get("redis.startDate");
            long startDateTimestamp = sdf.parse(startDateStr).getTime();
            //结束时间
            String endDateStr = configMap.get("redis.endDate");
            long endDateTimestamp = sdf.parse(endDateStr).getTime();

            Calendar calendar = Calendar.getInstance();
            calendar.setTime(sdf.parse(startDateStr));
            calendar.add(Calendar.DATE,1);
            Date nextDay = calendar.getTime();
            String nextDayStr = sdf.format(nextDay);
            long nextDayTimestamp = nextDay.getTime();
            while(startDateTimestamp < endDateTimestamp){
                for(String key : ckeys){
                    List<Sample.Value> valueList = rts.range(key, simpleDateFormat.parse(startDateStr + " 00:00:00").getTime(), simpleDateFormat.parse(nextDayStr + " 23:59:59").getTime());
                    double tempValue = 0;
                    long tempTempstamp = 0;
                    List<RedisTimeseriesTagMessage> redisTimeseriesTagMessageList = new ArrayList<>();
                    RedisTimeseriesTagMessage redisTimeseriesTagMessage = new RedisTimeseriesTagMessage();
                    for(Sample.Value value : valueList){
                        tempValue = value.getValue();
                        tempTempstamp = value.getTimestamp();
                        redisTimeseriesTagMessage.setKey(key);
                        redisTimeseriesTagMessage.setValue(tempValue);
                        redisTimeseriesTagMessage.setTimestamp(tempTempstamp);
                        redisTimeseriesTagMessageList.add(redisTimeseriesTagMessage);
                    }

                    ctx.collect(redisTimeseriesTagMessageList);
                }
                startDateStr = nextDayStr;
                startDateTimestamp = nextDayTimestamp;
            }
            logger.info("所有数据处理完成");

        }else{
            logger.info("程序非第一次执行，只处理前一天的数据开始");
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(new Date());
            calendar.add(Calendar.DATE,-1);
            Date date = calendar.getTime();
            Long beginDateTimeStamp = simpleDateFormat.parse(sdf.format(date) + " 00:00:00").getTime();
            Long endDateTimeStamp = simpleDateFormat.parse(sdf.format(date) + " 23:59:59").getTime();
            for(String key : ckeys){
                List<Sample.Value> valueList = rts.range(key, beginDateTimeStamp,endDateTimeStamp);
                double tempValue = 0;
                long tempTempstamp = 0;
                List<RedisTimeseriesTagMessage> redisTimeseriesTagMessageList = new ArrayList<>();
                RedisTimeseriesTagMessage redisTimeseriesTagMessage = new RedisTimeseriesTagMessage();
                for(Sample.Value value : valueList){
                    tempValue = value.getValue();
                    tempTempstamp = value.getTimestamp();
                    redisTimeseriesTagMessage.setKey(key);
                    redisTimeseriesTagMessage.setValue(tempValue);
                    redisTimeseriesTagMessage.setTimestamp(tempTempstamp);
                    redisTimeseriesTagMessageList.add(redisTimeseriesTagMessage);
                }
                ctx.collect(redisTimeseriesTagMessageList);
            }
            logger.info("数据处理完成");
        }
    }


    @Override
    public void cancel() {
        //关闭连接
        rtsc.shutdown();
    }

    public static List<String> getRedisClusterKeys(String[] redisHosts) {
        List<String> listAll = new ArrayList<String>();
        for (String str : redisHosts) {
            String hostname = str.split(":")[0];
            int port = Integer.parseInt(str.split(":")[1]);
            Jedis jedis = new Jedis(hostname, port);
            Client client = jedis.getClient();
            client.info();
            String info = client.getBulkReply();
            String info1 = info.trim().split("role:")[1].substring(0, 6).trim();
            if (info1.equals("master")) {//判断该redis是否是主数据库，是就执行keys获取，不是就不执行
                listAll.addAll(getSingleRedisKeys(hostname, port));
            }
            jedis.close();
        }
        return listAll;
    }

    public static List<String> getSingleRedisKeys(String hostname, int port) {
        Jedis jedis = new Jedis(hostname, port);
        // 游标初始值为0
        String cursor = ScanParams.SCAN_POINTER_START;
        String key = "*";
        ScanParams scanParams = new ScanParams();
        scanParams.match(key);
        scanParams.count(1000);
        List<String> listAll = new ArrayList<String>();
        while (true) {
            //使用scan命令获取数据，使用cursor游标记录位置，下次循环使用
            ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
            cursor = scanResult.getCursor();
            List<String> list = scanResult.getResult();
            listAll.addAll(list);
            if ("0".equals(cursor)) {
                break;
            }
        }
        jedis.close();
        return listAll;
    }

    private RedisTimeSeriesClient initConnect() {
        RedisTimeSeriesClient rtsc = null;
        Map<String, String> map = ConfigUtil.readConfig_properties(appPropertiesPath);
        String hostsStr = map.get("redis.hosts");
        String[] redisHosts = hostsStr.split(",");
        org.redisson.config.Config redissonCfg = new org.redisson.config.Config();
        ClusterServersConfig clusterCfg = redissonCfg.useClusterServers();
        for (String rConnItem : redisHosts) {
            clusterCfg.addNodeAddress("redis://" + rConnItem);
        }
        rtsc = new RedisTimeSeriesClient(redissonCfg);
        return rtsc;
    }

}
