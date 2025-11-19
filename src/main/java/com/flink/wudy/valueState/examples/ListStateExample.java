package com.flink.wudy.valueState.examples;


import com.flink.wudy.valueState.function.MySqlSinkFunction;
import com.flink.wudy.valueState.model.InputModel;
import com.flink.wudy.valueState.source.ProductSaleModelSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink状态接口
 * 统一的DataStream API，通过Flink状态接口构建具有 状态本地化、状态持久化、精确一次 的数据处理能力
 * Flink根据状态是否需要通过key进行分类，将状态分为算子状态和键值状态，区别：在于访问和更新状态值时状态值的作用域不同
 *
 * > 算子状态：在状态本地化后，每个SubTask只能访问到SubTask本地的状态数据，访问和更新状态值的范围被限定在当前SubTask中。作用域为当前算子的单个SUbTask的状态被FLink归类为算子状态
 *   >> 列表状态(ListState) - 平均分割重分布策略 和 合并列表状态(UnionListState) - 合并重分布策略  都属于算子状态。
 *   >>
 *
 * > 键值状态: 作用域为一个SubTask下的一个key的状态被Flink成为 键值状态（只有在KeyedStream上才能使用键值状态）
 *  >>> Flink DataStream API 提供了5中键值状态接口， ValueState(值状态)、MapState（映射状态）、ListState（列表状态）、ReducingState（规约状态）、AggregatingState(聚合状态)
 *
 * CheckPointedFunction接口包含以下方法:
 * > void initializeState(FunctionInitializationContext context) 在Flink作业启动或者异常容错从快照恢复时调用该方法
 *  >> FunctionInitializationContext提供了以下4个方法:
 *      - boolean isRestored(): 用于判断当前作业是否从快照恢复，如果是则返回true
 *      - OptionalLong getRestoredCheckpointId() 如果当前作业是从某个快照恢复的，则可以用该方法获取该快照的id
 *      - OperatorStateStore getOperatorStateStore() : 用于获取算子状态的状态存储器 OperatorStateStore, 内部提供了getListState 与 getUnionListState
 *      - KeyedStateStore getKeyedStateStore(): 用于获取键值状态的状态存储器 KeyedStateStore
 *      - void snapshotState(FunctionSnapshotContext context) : 在Flink作业执行快照时调用这个方法，FunctionSnapshotContext为用户自定义函数的快照上下文
 *        >>> long  getCheckpointId(): 返回当前快照的id， 快照的id是单调递增的，快照B的id如果大于快照A的id， 则说明B是在快照A之后执行的
 *        >>> long getCheckpointTimestamp(): 快照的执行有JobManager统一调度并触发，该方法可以返回JobManager开始执行当前快照的时间戳
 *
 * > 通过状态句柄ListState<T> 访问和更新状态数据
 *      在流处理场景中，假如我们使用MySQL作为数据汇存储引擎，当使用SinkFunction写入数据到MySQL，每来一条数据就向MySQL写入一条数据，那么大流量、频繁写入MySQL会导致稳定性问题
 *      解决方案：在数据汇算子中积累一批数据后批量写出，降低写入MySQL的写入频次, 我们可以使用ListState保存这批数据
 *
 * 如下例：使用ListState实现批量写数据到数据汇的BatchSinkFunction
 */
public class ListStateExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(ParameterTool.fromArgs(args).getConfiguration());
        env.setParallelism(2);

        // 1.启用检查点功能，触发间隔为10秒，设定了精确一次（Exactly-Once）的语义保证
        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);

        DataStreamSource<InputModel> productStream = env.addSource(new ProductSaleModelSource(), "product_sale_name");

        DataStreamSink<InputModel> productSinkStream = productStream.addSink(new MySqlSinkFunction(10)).name("product_sale_sink");

        env.execute("product sale execute job");
    }

}
