package com.gmh.cdc.state;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * 每2条数据 打印输出一次一次统计
 */
public class OperatorStateDemo02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1.1 开启CK并指定状态后端为FS,test-jdbc1-checkpoint-211216 目录会自己创建
        env.setStateBackend(new FsStateBackend("file:///d:/chuangke/checkpoints"));

        // 5s 做一次 CK
        env.enableCheckpointing(500L);
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


        DataStreamSource<Tuple2<String, Integer>> dataStreamSource =
                env.fromElements(Tuple2.of("A", 2), Tuple2.of("A", 3),
                        Tuple2.of("B", 5), Tuple2.of("B", 7), Tuple2.of("C", 1));

        dataStreamSource
                .addSink(new BufferingSink(2)).setParallelism(1);

        env.execute("OperatorStateDemo");
    }
}

class BufferingSink
        implements SinkFunction<Tuple2<String, Integer>>,
        CheckpointedFunction {
    //阈值
    private final int threshold;

    private transient ListState<Tuple2<String, Integer>> checkpointedState;
    //缓存
    private List<Tuple2<String, Integer>> bufferedElements;

    public BufferingSink(int threshold) {
        this.threshold = threshold;
        this.bufferedElements = new ArrayList<Tuple2<String, Integer>>();
    }

    //每一个元素都会调用这个方法
    public void invoke(Tuple2<String, Integer> value, Context contex) throws Exception {
        //放入缓存
        bufferedElements.add(value);
        //判断是否大于阈值
        if (bufferedElements.size() == threshold) {

            //统计总数
            System.out.println(bufferedElements.stream().reduce((a, b) -> Tuple2.of(value.f0,a.f1 + b.f1)).get());
            //清除缓存
            bufferedElements.clear();
            throw new RuntimeException();
        }
    }

    //快照  也就是对buff里面的数据进行保存
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkpointedState.clear();
        for (Tuple2<String, Integer> element : bufferedElements) {
            checkpointedState.add(element);
        }
    }

    //初始化或者恢复这个state
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Tuple2<String, Integer>> descriptor =
                new ListStateDescriptor<Tuple2<String, Integer>>(
                        "buffered-elements",
                        TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
                        }));

        checkpointedState = context.getOperatorStateStore().getListState(descriptor);

        if (context.isRestored()) {
            for (Tuple2<String, Integer> element : checkpointedState.get()) {
                bufferedElements.add(element);
            }
        }
    }
}
