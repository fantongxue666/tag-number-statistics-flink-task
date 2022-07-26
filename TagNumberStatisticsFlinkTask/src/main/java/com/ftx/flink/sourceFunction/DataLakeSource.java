package com.ftx.flink.sourceFunction;
import com.ftx.flink.model.DataLakeTagMessage;
import com.ftx.flink.utils.ConfigUtil;
import com.ftx.flink.utils.ConnectionUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

/**
 * 数据湖的源数据
 */
public class DataLakeSource extends RichSourceFunction<List<DataLakeTagMessage>> {

    final Logger logger = LoggerFactory.getLogger(DataLakeSource.class);

    //application.properties文件的绝对路径
    private String appPropertiesPath = StringUtils.EMPTY;
    //建立的数据湖连接
    private Connection connection = null;
    //是否是第一次进入
    boolean state = false;
    // 声明一个布尔变量，作为控制数据生成的标识位
    private Boolean running = true;

    /**
     * 初始化
     * @param appPropertiesPath
     */
    public DataLakeSource(String appPropertiesPath) {
        this.appPropertiesPath = appPropertiesPath;
    }

    @Override
    public void run(SourceContext<List<DataLakeTagMessage>> ctx) throws Exception {
        //初始化连接
        this.connection = ConnectionUtil.getIcebergConnection(appPropertiesPath);
        if(connection == null){
            logger.error("Connection to data source of DataLake failed");
        }
        Statement statement = connection.createStatement();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat sdf_hms = new SimpleDateFormat("HH:mm");
        while (running) {
            while (true){
                //获取当前时间是否是00：30
                String format = sdf_hms.format(new Date());
                if(StringUtils.equals(format,ConfigUtil.readConfig_properties(appPropertiesPath).get("executeTime"))){
                    break;
                }else{
                    Thread.sleep(1000 * 60);
                }
            }
            if (!state) {
                logger.info("程序第一次执行，处理数据湖的所有数据");
                //数据源为湖里的所有数据，每次推送一天的数据
                Map<String, String> propFromFile = ConfigUtil.readConfig_properties(appPropertiesPath);
                String startDateStr = propFromFile.get("startDate");
                String endDateStr = propFromFile.get("endDate");
                //开始时间
                Date startDate = sdf.parse(startDateStr);
                //结束时间
                Date endDate = sdf.parse(endDateStr);
                while ((startDate.compareTo(endDate)) != 1) {
                    //查一天的位号列表
                    String queryAllKeysByDay = "select tag_no from iceberg_yulin_phd where dt= '" + startDateStr + "' group by tag_no;";
                    ResultSet resultSet = statement.executeQuery(queryAllKeysByDay);
                    while (resultSet.next()) {
                        String tag_no = resultSet.getString("tag_no");
                        //查某个位号列表在那一天的所有历史数据
                        String sql = "select * from iceberg.yulin.phd where tag_no = '"+ tag_no + "' and dt = '" + startDateStr + "';";
                        ResultSet executeQuerySet = statement.executeQuery(sql);
                        List<DataLakeTagMessage> dataLakeTagMessageList = new ArrayList<>();
                        while (executeQuerySet.next()){
                            String dt = executeQuerySet.getString("dt");
                            Timestamp ts = executeQuerySet.getTimestamp("ts");
                            String tag_no1 = executeQuerySet.getString("tag_no");
                            String phd_tag = executeQuerySet.getString("phd_tag");
                            String device_id = executeQuerySet.getString("device_id");
                            Long date_time = executeQuerySet.getLong("date_time");
                            Double tag_value = executeQuerySet.getDouble("tag_value");
                            Integer confidence = executeQuerySet.getInt("confidence");
                            Long formatted_time = executeQuerySet.getLong("formatted_time");
                            String data_type = executeQuerySet.getString("data_type");
                            DataLakeTagMessage dataLakeTagMessage = new DataLakeTagMessage();
                            dataLakeTagMessage.setDt(sdf.parse(dt));
                            dataLakeTagMessage.setTs(new Date(ts.getTime()));
                            dataLakeTagMessage.setTagNo(tag_no1);
                            dataLakeTagMessage.setPhdTag(phd_tag);
                            dataLakeTagMessage.setDeviceId(device_id);
                            dataLakeTagMessage.setDateTime(date_time);
                            dataLakeTagMessage.setTagValue(tag_value);
                            dataLakeTagMessage.setConfidence(confidence);
                            dataLakeTagMessage.setFormattedTime(formatted_time);
                            dataLakeTagMessage.setDataType(data_type);
                            dataLakeTagMessageList.add(dataLakeTagMessage);
                        }
                        ctx.collect(dataLakeTagMessageList);
                    }
                    //startDate增加一天
                    Calendar calendar = Calendar.getInstance();
                    calendar.setTime(startDate);
                    calendar.add(Calendar.DATE,1);
                    Date newStartTime = calendar.getTime();
                    startDate = newStartTime;
                    startDateStr = sdf.format(startDate);
                }
                //执行完成后状态置为True
                state = true;
                logger.info("数据处理完成");
            }else{
                //数据源为前一天的历史数据，直接发送全部的，交给flink程序处理
                Calendar calendar = Calendar.getInstance();
                calendar.setTime(new Date());
                calendar.add(Calendar.DATE,-1);
                Date lastDate = calendar.getTime();
                String lastDateStr = sdf.format(lastDate);
                //查一天的位号列表
                String queryAllKeysByDay = "select tag_no from iceberg_yulin_phd where dt= '" + lastDateStr + "' group by tag_no;";
                ResultSet resultSet = statement.executeQuery(queryAllKeysByDay);
                while (resultSet.next()) {
                    String tag_no = resultSet.getString("tag_no");
                    //查某个位号列表在那一天的所有历史数据
                    String sql = "select * from iceberg.yulin.phd where tag_no = '"+ tag_no + "' and dt = '" + lastDateStr + "';";
                    ResultSet executeQuerySet = statement.executeQuery(sql);
                    List<DataLakeTagMessage> dataLakeTagMessageList = new ArrayList<>();
                    while (executeQuerySet.next()){
                        String dt = executeQuerySet.getString("dt");
                        Timestamp ts = executeQuerySet.getTimestamp("ts");
                        String tag_no1 = executeQuerySet.getString("tag_no");
                        String phd_tag = executeQuerySet.getString("phd_tag");
                        String device_id = executeQuerySet.getString("device_id");
                        Long date_time = executeQuerySet.getLong("date_time");
                        Double tag_value = executeQuerySet.getDouble("tag_value");
                        Integer confidence = executeQuerySet.getInt("confidence");
                        Long formatted_time = executeQuerySet.getLong("formatted_time");
                        String data_type = executeQuerySet.getString("data_type");
                        DataLakeTagMessage dataLakeTagMessage = new DataLakeTagMessage();
                        dataLakeTagMessage.setDt(sdf.parse(dt));
                        dataLakeTagMessage.setTs(new Date(ts.getTime()));
                        dataLakeTagMessage.setTagNo(tag_no1);
                        dataLakeTagMessage.setPhdTag(phd_tag);
                        dataLakeTagMessage.setDeviceId(device_id);
                        dataLakeTagMessage.setDateTime(date_time);
                        dataLakeTagMessage.setTagValue(tag_value);
                        dataLakeTagMessage.setConfidence(confidence);
                        dataLakeTagMessage.setFormattedTime(formatted_time);
                        dataLakeTagMessage.setDataType(data_type);
                        dataLakeTagMessageList.add(dataLakeTagMessage);
                    }
                    if(!dataLakeTagMessageList.isEmpty()){
                        ctx.collect(dataLakeTagMessageList);
                    }
                }
            }
            Thread.sleep(1000 * 60 * 5);
        }
    }

    @Override
    public void cancel() {
        //关闭连接
        try {
            connection.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

}
