package com.gmh.cdc;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import javafx.stage.StageStyle;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class FlinkStreamCdcOne {

    public static void main(final String[] args) throws Exception {

        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.1 开启CK并指定状态后端为FS,test-jdbc1-checkpoint-211216 目录会自己创建
        env.setStateBackend(new FsStateBackend("file:///mnt/data/flink/checkpoint"));

        // 5s 做一次 CK
        env.enableCheckpointing(5000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // ck 的超时时间 10s
        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        // 允许检查点最大并发，当前一个检查点延时，允许在规定的时间点在开启一个检查点，如5s开启的检查点，延时到14s，那10s的时候允许在启动一个检查点，这就2个检查点了
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        // 最小间隔时间，如5s开启的检查点，延时到14s，最小间隔时间为2s，也就是16s开启另一个检查点，不会存在 并发检查点的问题。
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000);

        // 老版本中 需要设置重启策略,新版本重启策略比较合理，老版本重启次数是Int的最大值
        // RestartStrategies.fixedDelayRestart()，                   //重启一次间隔一定时间重启第二次，直到重启次数以参数限制为准
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000));

        Properties properties = new Properties();
        properties.setProperty("scan.startup.mode", "initial");
        MySqlSource<String> mysqlSource = MySqlSource.<String>builder()
                .hostname("114.67.101.133")
                .port(3306)
                .username("root")
                .password("jd@gmh#mysql")
                // 读取哪个库，可以读取多个库，默认监控库下所有表
                .databaseList("fangao")
                // 监控库下的某些表 test_jdbc1.table,test_jdbc1.table1
                .tableList("fangao.uc_study_video_new_topic")
                // 反序列化  用的是 Debezium 的 StringDebeziumDeserializationSchema() 格式不方便，所以要自定义
                .deserializer(new StringDebeziumDeserializationSchema())
                // 启动参数 提供了如下几个静态方法
                // StartupOptions.initial() 第一次启动的时候，会把历史数据读过来（全量）做快照，后续读取binlog加载新的数据，如果不做 chackpoint 会存在重启又全量一遍。
                // StartupOptions.earliest() 只从binlog开始的位置读（源头），这里注意，如果binlog开启的时间比你建库时间晚，可能会读不到建库语句会报错，earliest要求能读到建表语句
                // StartupOptions.latest() 只从binlog最新的位置开始读
                // StartupOptions.specificOffset() 自指定从binlog的什么位置开始读
                // StartupOptions.timestamp() 自指定binlog的开始时间戳
                .startupOptions(StartupOptions.initial())
                .debeziumProperties(properties)
                .build();

        env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "mysql")
                .map(new MapFunction<String, Object>() {

                    @Override
                    public Object map(String s) throws Exception {
                        if (s.contains("11")) {
                            System.out.println("执行异常");
                            throw new RuntimeException();
                        }
                        return s;
                    }
                }).setParallelism(1).print().setParallelism(1);
        // 数据读取时进行operator state
        /*DataStreamSource<Integer> topic = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9);
        topic.map(new MapFunction<Integer, Object>() {

            @Override
            public Object map(Integer integer) throws Exception {
                if (integer == 4) {
                    System.out.println(1/0);
                }
                return integer;
            }
        }).print();*/

        // 4. 启动任务
        env.execute("flink-cdc");
    }

}