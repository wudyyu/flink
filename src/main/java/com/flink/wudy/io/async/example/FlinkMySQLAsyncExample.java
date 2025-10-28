package com.flink.wudy.io.async.example;

import com.flink.wudy.io.async.Request.AsyncDatabaseRequest;
import com.flink.wudy.io.async.Request.MysqlAsyncRichMapFunction;
import com.flink.wudy.io.async.source.UserDefinedSource;
import com.flink.wudy.sink.kafka.model.CustomInputModel;
import com.flink.wudy.sink.strategy.forward.model.InputModel;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * 1.在处理数据的时候，会访问一些外部接口获取扩展数据，在Flink中成为I/O处理
 * 2.区别于 Flink作业中Source算子从数据源存储引擎中获取数据以及Sink算子将数据写入数据汇存储引擎的I/O不同，上面的I/O是指
 *   在Map、FlatMap等算子中处理数据时访问外部的数据库(MySQL、Redis)引擎
 *
 * 3.同步I/O处理导致作业低吞吐 的4种解决方案
 * > 提高Map算子的并行度(缺点: 算子并行度越大需要Flink作业分配的资源越多，需要更多的TaskManager)
 * > 使用本地缓存
 * > 批量访问外部存储
 * > 转换为异步I/O处理
 *
 * 4.异步I/O处理API
 *  Flink异步I/O处理API和DataStream进行了良好的集成，为了实现与外部数据库进行异步I/O交互，通常
 *  > 需要数据库提供可以支持异步请求的客户端(主流数据库都有提供)，Flink算子使用异步I/O客户端发起异步请求
 *  > 如果数据库没有提供异步客户端，也可以通过使用线程池同步调用的方式，将同步调用过程转换为异步调用
 *
 *  异步I/O产生数据顺序保障:
 *  > AsyncDataStream.unorderedWait() - 无序模式,异步I/O请求结束就会立马发出结果结果
 *
 *  > AsyncDataStream.orderedWait() - 有序模式，异步I/O算子会将第二个请求的结果保存下来，待第一个请求的计算结果发出后，才发出第二条数据的计算结果
 *    场景: 对数据产出顺序有严格要求，例如： 场景商品订单、用户支付商品订单、商品订单退款
 *
 *  无序模式与事件时间关系
 *  > Flink中包含事件时间的时间窗口算子，由于时间窗口算子对于输入数据的乱序非常敏感，但是计算结果不回产生错误
 *  > 当使用了无序模式的异步I/O算子时，异步I/O算子可以通过Watermark建立数据产出顺序的边界，相邻两个Watermark之间的数据可能是无序的，但同一个Watermark前后数据依然有序
 *  例如:在无序模式下, 异步I/O算子输入数据顺序为 A、B、C、D ， 在B、C之间有一个Watermark时， 输出的顺序可能为 B、A、C、D 或者 B、A、D、C
 *  但不可能为 A、C、B、D，
 *  一定是先输出 A、B  再输出 C、D， 这样就能保障事件时间语义下时间窗口算子计算的准确性
 *
 *  异步客户端MySQL：
 *  sudo /usr/local/mysql/support-files/mysql.server start
 *  执行命令：   mysql  -uroot -p
 *
 *
 */
public class FlinkMySQLAsyncExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(ParameterTool.fromArgs(args).getConfiguration());
        DataStreamSource<CustomInputModel> source = env.addSource(new UserDefinedSource());

        // 方法1: AsyncDataStream.unorderedWait
        SingleOutputStreamOperator<Tuple2<CustomInputModel, String>> transformation =
        AsyncDataStream.unorderedWait(
                source,
                new AsyncDatabaseRequest(),
                10000,    //异步超时时间
                TimeUnit.MILLISECONDS,
                100  // 最大异步并发请求数量
        );
        transformation.print();
//        transformation.addSink(new MysqlAsyncRichMapFunction()).name("test Async");
        env.execute("Async task");
        
    }
}
