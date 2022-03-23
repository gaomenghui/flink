package com.gmh.cdc.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Operator State Demo
 *
 * 1 2 3 4 7 5 1 5 4 6 1 7 8 9 1
 *
 * 输出如下：
 * (5,2 3 4 7 5)
 * (3,5 4 6)
 * (3,7 8 9)
 */
public class OperatorStateDemo {


    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Long> input=env.fromElements(1L,2L,3L,4L,7L,5L,1L,5L,4L,6L,1L,7L,8L,9L,1L);

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

        input.flatMap(new OperatorStateMap()).setParallelism(1).print();

        System.out.println(env.getExecutionPlan());

        env.execute();
    }


    public static class OperatorStateMap extends RichFlatMapFunction<Long, Tuple2<Integer,String>> implements CheckpointedFunction {

        //托管状态
        private ListState<Long> listState;
        //原始状态
        private List<Long> listElements;

        @Override
        public void flatMap(Long value, Collector collector) throws Exception {
            if(value==1){
                if(listElements.size()>0){
                    StringBuffer buffer=new StringBuffer();
                    for(Long ele:listElements){
                        buffer.append(ele+" ");
                    }
                    int sum=listElements.size();
                    collector.collect(new Tuple2<Integer,String>(sum,buffer.toString()));
                    listElements.clear();
                }
            }else{
                listElements.add(value);
            }

            /*if (value == 4) {
                System.out.println("执行异常");
                throw new RuntimeException();
            }*/
        }

        /**
         * 进行checkpoint进行快照
         * @param context
         * @throws Exception
         */
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            listState.clear();
            for(Long ele:listElements){
                listState.add(ele);
            }
        }

        /**
         * state的初始状态，包括从故障恢复过来
         * @param context
         * @throws Exception
         */
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor listStateDescriptor=new ListStateDescriptor("checkPointedList",
                    TypeInformation.of(new TypeHint<Long>() {}));
            listState=context.getOperatorStateStore().getListState(listStateDescriptor);
            //如果是故障恢复
            if(context.isRestored()){
                //从托管状态将数据到移动到原始状态
                for(Long ele:listState.get()){
                    listElements.add(ele);
                }
                listState.clear();
            }
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            listElements=new ArrayList<Long>();
        }
    }

}
