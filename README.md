# Tag Number Statistics FlinkTask

#### 介绍
Flink实现统计历史数据的一个执行任务

#### 软件架构
##### 1，Source

从redis和数据湖分别读取历史数据

##### 2，Flink任务

- 每天凌晨00:00执行程序，统计缺少数据的位号
- 第一次执行统计数据源的位号的所有历史数据，以后执行就只统计前一天的历史数据
- 统计所有历史数据的过程也是开窗（一天）进行批处理

##### 3，Sink

统计结果写入数据湖

##### 4，统计结果的表设计

表一

| ID                | 位号名称   | 位号日期   | 所属实例 | 统计日期   | 是否缺少 |
| ----------------- | ---------- | ---------- | -------- | ---------- | -------- |
| 位号名称+位号日期 | 11TT-10121 | yyyy-MM-dd | XXXXXXX  | yyyy-MM-dd | TRUE     |

表二

| ID   | 表一ID            | 缺少时刻            |
| ---- | ----------------- | ------------------- |
| UUID | 位号名称+位号日期 | yyyy-MM-dd HH:mm:ss |

##### 5，如何统计

##### 6，配置项




#### 安装教程

1.  xxxx
2.  xxxx
3.  xxxx

#### 使用说明

1.  xxxx
2.  xxxx
3.  xxxx

#### 参与贡献

1.  Fork 本仓库
2.  新建 Feat_xxx 分支
3.  提交代码
4.  新建 Pull Request


#### 特技

1.  使用 Readme\_XXX.md 来支持不同的语言，例如 Readme\_en.md, Readme\_zh.md
2.  Gitee 官方博客 [blog.gitee.com](https://blog.gitee.com)
3.  你可以 [https://gitee.com/explore](https://gitee.com/explore) 这个地址来了解 Gitee 上的优秀开源项目
4.  [GVP](https://gitee.com/gvp) 全称是 Gitee 最有价值开源项目，是综合评定出的优秀开源项目
5.  Gitee 官方提供的使用手册 [https://gitee.com/help](https://gitee.com/help)
6.  Gitee 封面人物是一档用来展示 Gitee 会员风采的栏目 [https://gitee.com/gitee-stars/](https://gitee.com/gitee-stars/)
