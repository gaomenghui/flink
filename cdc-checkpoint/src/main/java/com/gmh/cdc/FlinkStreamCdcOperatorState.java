package com.gmh.cdc;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.commons.math3.fitting.leastsquares.EvaluationRmsChecker;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.kafka.common.protocol.types.Field;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class FlinkStreamCdcOperatorState {

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


        // 2. 通过FlinkCDC构建SourceFunction并读取数据

        MySqlSource<JSONObject> topic = getSource("uc_study_video_new_topic");
        DataStreamSource<JSONObject> topicSource = env.fromSource(topic, WatermarkStrategy.noWatermarks(), "topic");

        /*topicSource.map(new MapFunction<JSONObject, JSONObject>() {

            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                return jsonObject;
            }
        }).print();*/
        topicSource.map(new OperatorStateMapFunction()).print();

        // 4. 启动任务
        env.execute("flink-cdc");
    }

    private static MySqlSource<JSONObject> getSource(String tableName) {
        //Properties properties = new Properties();
        //properties.setProperty("scan.incremental.snapshot.chunk.size", "777770");
        return MySqlSource.<JSONObject>builder()
                .hostname("114.67.101.133")
                .port(3306)
                .username("root")
                .password("jd@gmh#mysql")
                // 读取哪个库，可以读取多个库，默认监控库下所有表
                .databaseList("fangao")
                // 监控库下的某些表 test_jdbc1.table,test_jdbc1.table1
                .tableList("fangao."+tableName)
                // 反序列化  用的是 Debezium 的 StringDebeziumDeserializationSchema() 格式不方便，所以要自定义
                .deserializer(new JsonDebeziumDeserializationSchema())
                // 启动参数 提供了如下几个静态方法
                // StartupOptions.initial() 第一次启动的时候，会把历史数据读过来（全量）做快照，后续读取binlog加载新的数据，如果不做 chackpoint 会存在重启又全量一遍。
                // StartupOptions.earliest() 只从binlog开始的位置读（源头），这里注意，如果binlog开启的时间比你建库时间晚，可能会读不到建库语句会报错，earliest要求能读到建表语句
                // StartupOptions.latest() 只从binlog最新的位置开始读
                // StartupOptions.specificOffset() 自指定从binlog的什么位置开始读
                // StartupOptions.timestamp() 自指定binlog的开始时间戳
                .startupOptions(StartupOptions.initial())
                //.debeziumProperties(properties)
                .build();
    }


    public static class OperatorStateMapFunction extends RichMapFunction<JSONObject, JSONObject> implements ListCheckpointed {

        private List<String> states =  new ArrayList<String>();

        @Override
        public JSONObject map(JSONObject jsonObject) throws Exception {
            boolean flag = true;
            for (String state : states) {
                if (state.equals(jsonObject.toJSONString())) {
                    System.out.println("数据已经处理，存在重复, 状态大小" + states.size() + " ===== " + jsonObject.toJSONString());
                    flag = false;
                }
            }
            states.add(jsonObject.toJSONString());
            return flag ? jsonObject : null;
        }

        @Override
        public List snapshotState(long l, long l1) throws Exception {
            return states;
        }

        @Override
        public void restoreState(List list) throws Exception {
            list.addAll(states);
        }
    }

}