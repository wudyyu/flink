package com.flink.wudy.valueState.examples;

import com.flink.wudy.valueState.function.ProductRichMapFunction;
import com.flink.wudy.valueState.model.InputModel;
import com.flink.wudy.valueState.source.InputModelSource;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink有状态计算的4类应用
 * > 1.分组聚合应用
 * >> Max,Min,Sum,Reduce 等操作的计算过程都是有状态计算
 * > 2.时间窗口应用
 * >> 时间窗口的计算过程也是有状态计算
 * > 3.用户自定义状态应用
 * > 4.机器学习应用
 *
 * Flink如何实现有状态计算
 * > 状态本地化、持久化实现极致的状态访问速度
 *  >> 持久化机制(快照机制)：Flink作业在运行时定时且自动将作业中的状态数据持久化到远程的分布式文件系统(HDFS)
 *
 * > 通过精准一次的一致性快找实现低成本的异常容错
 *  >> 在持久化基础上，分布式系统快照算法作为理论基础，实现CheckPoint的分布式轻量级异步快照,保证精确一次的数据处理和一致性状态
 *     CheckPoint实现了处理数据逻辑和异常容错逻辑之间的解耦，只需要使用Flink状态接口，并开启CheckPoint机制，Flink作业就能自动实现精确一次
 *
 * > 通过统一的状态接口提升状态的易用性
 *  >> Flink在状态本地化、持久化以及CheckPoint机制的基础上，基于DataStream API提供了一套标准易用的状态接口
 *
 * Flink实现有状态计算面临的2个难题
 * > 作业横向扩展、升级时的状态恢复问题
 *  >> 业务流量的变化，我们可能需要增加/减小 Flink作业中的算子并行度
 *     - 有状态算子并行度变化时的状态重分布问题
 *       >>>  KeyBy/Map算子的并行度为2，两个SubTask分别在本地维护状态数据，如果算子并行度变为3，如何将原本在两个SubTask中维护的状态数据分配到3个SubTask中？ 这就是有状态算子并行度变化时的状态重分布问题
 *          场景1：算子状态：Flink提供了 《平均分割的状态重分布策略》 和 《合并状态重分布策略》
 *          场景2：键值状态：算子最大并行度和Key-Group（键-组）的方案，实现了在作业横向扩展后自动将每个key的状态数据按照key的分发策略重分布到新的SubTask
 *     - 增、减有状态算子时的状态恢复问题
 *       >>> 针对增、减有状态算子的状态恢复问题，Flink提出了SavePoint（保存点）机制, 核心功能：由用户给每个算子添加一个唯一的id，当算子的SubTask生成快照时，会给当前算子的状态数据标记这个id，后续添加/删除有状态算子都是按照id进行数据恢复
 *
 * > 状态本地化后大状态存储问题
 *  >> 状态的应用场景如下
 *   >>> 缓存场景: 键值状态保留时长功能。Flink预置了HashMap（内存）、RocksDB（磁盘）两种类型的状态后端
 *   >>> 存储场景
 *
 *
 *
 * 案例：
 *  电商场景中统计每种商品的累计销售额，输入数据流InputModel(包含productId(商品id),income(商品销售额),timestamp字段)
 *
 */
public class ValueStateExample {
    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        config.setString("taskmanager.memory.process.size", "1024m");
        config.setString("taskmanager.memory.task.heap.size", "512m");
        config.setString("taskmanager.numberOfTaskSlots", "2");
        config.setString("akka.ask.timeout", "60s");
        config.setString("akka.tcp.timeout", "60s");
        config.setString("rest.port", "8081");

//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        // 1.启用检查点功能，触发间隔为10秒，设定了精确一次（Exactly-Once）的语义保证
        env.enableCheckpointing(30000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);

        // 2.配置checkpoint快照保存在分布式文件目录
        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage("file:/Users/wudy.yu/flink/checkpoints"));

        DataStream<InputModel> transformation = env.addSource(new InputModelSource())
                .keyBy(InputModel::getProductId)
                .map(new RichMapFunction<InputModel, InputModel>() {
                    // 3.定义ValueState状态描述符
                    private final ValueStateDescriptor<Long> cumulateIncomeStateDescriptor = new ValueStateDescriptor<Long>("cumulate income", Long.class);
                    private transient ValueState<Long> cumulateIncomeState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        // 4.从RichFunction的上下文获取状态句柄
                        this.cumulateIncomeState = this.getRuntimeContext().getState(cumulateIncomeStateDescriptor);
                    }

                    @Override
                    public InputModel map(InputModel inputModel) throws Exception {
                        // 通过ValueState提供的T value() 方法访问状态中保存的历史累计结果
                        Long cumulateIncome = this.cumulateIncomeState.value();
                        if (null == cumulateIncome) {
                            // 如果是当前key下的第一条数据，则从状态中访问到的数据为null
                            cumulateIncome = inputModel.getIncome();
                        } else {
                            cumulateIncome += inputModel.getIncome();
                        }
                        // 通过ValueState提供的void update() 方法将当前商品累计销售额更新到状态中
                        this.cumulateIncomeState.update(cumulateIncome);
                        inputModel.setIncome(cumulateIncome);
                        return inputModel;
                    }
                });

        DataStreamSink<InputModel> sink = transformation.print();

        env.execute("Product Value State");

    }
}
