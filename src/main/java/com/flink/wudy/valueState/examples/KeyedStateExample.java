package com.flink.wudy.valueState.examples;

import com.flink.wudy.valueState.function.MySqlSinkFunction;
import com.flink.wudy.valueState.function.ProductKeyedRichMapFunction;
import com.flink.wudy.valueState.model.InputModel;
import com.flink.wudy.valueState.source.ProductSaleModelSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 键值状态：
 * > 键值状态是在算子状态的基础上演化而来的一种状态类型，只有在keyedStream上的算子才能使用键值状态，作用域比算子状态更小，仅是单个SubTask的key
 * >应用场景:
 *  >> 在数据源算子中保存偏移量
 *  >> 在数据汇算子中保存一批数据,进行批量写出
 *
 * > 键值状态5种分类
 *  >> ValueState （值状态）
 *    >>> T value() 用于获取状态值
 *    >>> void update(T value) 用于更新状态值
 *  >> MapState (映射状态)
 *    >>> boolean contains(UK key) 判断MapState中是否包含如惨key
 *    >>> Iterable<Map.Entry<UK, UV>> entries()  从MapState只能够获取当前存储的所有键值对
 *    >>> Iterable<UK> keys()    从MapState中获取当前存储的所有键
 *    >>> Iterable<UV> values()  从MapState中获取当前存储的所有value
 *    >>> Iterable<Map.Entry<UK, UV>> iterator() 从MapState中获取当前存储的所有键值对的迭代器
 *    >>> boolean isEmpty()
 *  >> ListState
 *  >> ReducingState (规约状态)
 *   >>> 对应 ReducingState<T> 接口，可以用于存储单一变量，其中T就是变量的数据类型, 提供了 void add(T) 方法向状态中添加元素
 *   >>> ReducingState 与 ListState 区别在于ListState会保存添加的所有元素列表，ReducingState在初始化时会通过状态描述符ReducingStateDescriptor传入一个ReduceFunction<T>
 *       每当使用void add(T)方法向ReducingState添加数据时，都会通过ReduceFunction<T>将历史状态值和新添加的元素值进行规约计算，得到新的状态值并保存
 *  >> AggregatingState （聚合状态）
 *   >>> 对应Flink中的AggregatingState<IN, OUT>接口,也是用于存储单一变量的状态
 *   >>> AggregatingState允许输入元素数据类型和输出结果类型不同， 描述符 AggregatingStateDescriptor<IN, ACC, OUT>需要传入一个AggregateFunction<IN,ACC,OUT>来定义数据聚合计算的逻辑
 *
 * > 5种键值状态之间的继承关系
 *                      State
 *                    |   ｜   \
 *                   |    ｜    \
 *                  |     ｜     \
 *         ValueState  MapState  AppendingState
 *                                     ｜
 *                                     ｜
 *                                MergingState
 *                              |     ｜      \
 *                             |      ｜       \
 *                            |       ｜        \
 *                     ListState   ReduceState AggregatingState
 *
 * > 键值状态(KeyedState)使用步骤
 *  >> 1.定义状态描述符
 *   >>> ValueStateDescriptor<T>
 *   >>> MapStateDescriptor<UK, UV>
 *   >>> ReducingStateDescriptor<T>
 *   >>> AggregatingStateDescriptor<IN,ACC,OUT>
 *
 *  >> 2.用户自定义函数(继承RichFunction抽象类),在RichFunction提供的运行时上下文中通过状态描述符获取状态句柄
 *       RichFunction提供的RuntimeContext getRuntimeContext()方法获取Flink作业的运行时上下文RuntimeContext
 *   >>> <T> ValueState<T> getState(ValueStateDescriptor<T> stateProperties) 获取ValueState状态句柄
 *   >>> <UK, UV> MapState<T> getMapState(MapStateDescriptor<T> stateProperties) 获取MapState状态句柄
 *   >>> <T> ListState<T> getListState(ListStateDescriptor<T> stateProperties) 获取ListState状态句柄
 *   >>> <T> ReducingState<T> getReducingState(ReducingStateDescriptor<T> stateProperties) 获取ReducingState状态句柄
 *   >>> <IN, ACC, OUT> AggregatingState<IN, OUT> getAggregatingState(AggregatingStateDescriptor<IN,ACC,OUT> stateProperties) 获取AggregatingState状态句柄
 *
 *
 *  >> 3.通过状态句柄访问、更新状态数据
 *
 * 案例：电商场景中计算每种商品的累计销售额,使用ValueState保存每一种商品的累计销售额
 */
public class KeyedStateExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(ParameterTool.fromArgs(args).getConfiguration());
        env.setParallelism(2);

        // 启用检查点功能，触发间隔为10秒，设定了精确一次（Exactly-Once）的语义保证
        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);

        DataStreamSource<InputModel> productSourceStream = env.addSource(new ProductSaleModelSource(), "product_sale_name");
        SingleOutputStreamOperator<InputModel> sinkStream = productSourceStream.keyBy(InputModel::getProductId).map(new ProductKeyedRichMapFunction()).name("Product Sale Map Task");
        DataStreamSink<InputModel> productSinkStream = sinkStream.addSink(new MySqlSinkFunction(1)).name("product_sale_sink");
        sinkStream.print().name("print out product sale");
        env.execute("product income summing");
    }
}
