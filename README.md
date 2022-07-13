# Tag Number Statistics FlinkTask

#### 介绍
统计历史数据的Flink任务，统计位号历史数据中哪些位号缺少某时间点的数据，生成湖数据或csv报表

#### 软件架构
##### 1，Source

从redis和数据湖分别读取历史数据

##### 2，Flink任务

- 每天凌晨00:00:20执行程序，统计缺少数据的位号
- 第一次执行统计数据源的位号的所有历史数据，以后执行就只统计前一天的历史数据
- 统计所有历史数据的过程也是开窗（一天）进行批处理

##### 3，Sink

统计结果写入数据湖

##### 4，统计结果的表设计

TableName：DeletionDataStat

| ID（id） | 位号名称（tagName） | 缺失日期（deletionDate） | 缺失数（deletionCount） | 缺失时刻列表（deletionTimestamps） |
| -------- | ------------------- | ------------------------ | ----------------------- | ---------------------------------- |
| UUID     | 11TT-10121          | yyyy-MM-dd               | xx                      | timestamp1,timestamp2,timestamp3   |

##### 5，如何统计

- 第一步，遍历位号，对每个位号查询前一天的列表
- 第二步，筛选出缺失数据
- 第三步，存入数据湖

##### 6，配置项
- 配置实例的位号的时间频度（例如：时间间隔是2s，每2s一条历史数据）查询统计的sql动态读取配置项信息进行调整
- 配置redis和数据湖的连接信息

配置位号时间频度的配置文件

```properties
# key：位号名称
# value：时间间隔 S|s 秒 M|m 分 H|h 时 支持小数
11TT-10121=60s\
11TT-10122=60s
...
```



#### 使用教程

1. 打包Flink程序，上传Flink客户端

2. 执行命令提交执行任务到Yarn集群

   ​	如：`flink run -d -t yarn-per-job -c com.atguigu.wc.StreamWordCount  FlinkTutorial-1.0-SNAPSHOT.jar`

3. 通过ApplicationMaster查看任务执行情况
