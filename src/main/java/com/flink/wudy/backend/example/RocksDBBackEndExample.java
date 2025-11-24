package com.flink.wudy.backend.example;

import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Flink状态后端（State BackEnd）
 *  > Flink作业开启CheckPoint机制后，作业的本地状态数据会随着CheckPoint的执行而持久化到远程分布式文件系统中
 *  > 状态后端用于管理本地状态数据的存储格式以及状态数据持久化的方式，Flink预置了以下两种状态后端
 *    >> HashMap(HashMapStateBackend) : 状态数据存储在SubTask内存中，访问速度快
 *          >>> 通过代码设置Flink状态后端:
 *              env.setStateBackend(new HashMapStateBackend()); // 指定状态后端为HashMap
 *              env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage("hdfs://namenode:40010/flink/checkpoints")); // Checkpoint快照文件存储目录
 *
 *          >>> 通过flink-conf.yaml设置状态后端
 *              state.backend: hashmap # 状态后端类型
 *              state.checkpoints.dir: hdfs://namenode:40010/flink/checkpoints  # heckpoint快照文件存储目录
 *
 *
 *
 *    >> RocksDB(EmbeddedRocksDBStateBackend): 状态数据存储在SubTask磁盘中，存储容量大
 *          >>> RocksDB是Facebook基于levelDB使用C++便携的K-V存储引擎
 *
 *  > 在Flink中使用RocksDB
 *    >> 1.作业启动RocksDB
 *      >> 当我们配置Flink作业的状态后端为RocksDB时，Flink作业算子的SubTask在初始化时，会在SubTask本地启动RocksDB，RocksDB会以原生线程的形式嵌入TaskManager进程
 *         和SubTask处理数据的线程并非同一个。RocksDB状态后端不使用JVM的堆存储运行中的状态，所以不受JVM垃圾回收的影响
 *    >> 2.访问RocksDB
 *         RocksDB是一个K-V存储引擎，只用于存储键值状态数据，算子状态数据依然会被存储在SubTask的内存中。
 *         在使用键值状态接口存储、更新和访问状态数据时，会由SubTask的线程通过JNI接口操作RocksDB
 *    >> 3.使用RocksDB存储状态数据
 *          使用RocksDB存储状态数据，RocksDB默认将数据存储在SubTask所在TaskManager的数据目录中，也就是本地磁盘
 *
 *  RocksDB状态后端配置
 *   > 1.使用作业代码配置
         StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
         // 设置状态后端为RocksDB
         env.setStateBackend(new EmbeddedRocksDBStateBackend());
         // 设置增量CheckPoint（生成环境推荐）
         env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
         // 配置CheckPoint快照文件的目录
         env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage("hdfs://namenode:40010/flink/checkpoints"));

    > 2.通过flink-conf.yaml配置
        state.backend: rocksdb  # 设置状态后端类型
        state.backend.incremental: true # 设置增量CheckPoint
        state.checkpoint.dir: hdfs://namenode:40010/flink/checkpoints # 设置CheckPoint快照文件的目录

 > RocksDB使用建议:
    > 1.处理大状态、长窗口有状态处理的作业
    > 2.注意细节:
        >> 状态数据序列化（必须经过序列化、反序列化）,状态数据先被序列化器序列化为字节数组byte[], 然后通过JNI接口写入RocksDB
        >> 状态访问性能: 相比HashMap状态后端性能更差，在Flink作业中重用状态数据是安全的
        >> 状态值比较： 以字节数组进行比较，而不是使用Java对象的hashCode()和equals()方法

 > RocksDB进阶配置:
    >> 1.RocksDB状态后端增量快照:  RocksDB状态后端是目前唯一支持增量快照(增量Checkpoint）的状态后端。增量快照默认关闭，使用以下配置开启：
     >>> env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
     >>> state.backend.incremental: true

    >> 2.定时器状态数据的存储
      >>> 定时器主要用来触发窗口计算，为了在作业异常时定时器不丢失，定时器会被存储到键值状态中
      >>> 存储定时器的数据结构是一个 支持去重的优先队列,当配置RocksDB为状态后端时，默认定时器会被存储在RocksDB，为了性能考虑我们一般会将定时器存储在JVM堆，
          故提供了以下配置: state.backend.rocksdb.timer-service.factory: heap
 > 状态后端注意事项：
    >> 区分键值状态和算子在状态
        >>> 算子状态只会存储在SubTask中，因此在生产环境中严格区分键值状态和算子状态的使用场景,避免因为将算子状态当作键值状态使用而出现内存溢出的问题
    >>ValueState<HashMap<String, String>> 还是 MapState<String, String>
        >>> 如果要在键值状态中存储Map<String, String>s数据结构的状态
            在HashMap状态后端时，两者区别不大
            在RocksDB状态后端时，推荐使用MapState<String, String>

 > Flink故障重启策略
    注意：如果没有在flink-conf.yaml，也没有在作业代码中设置重启策略
         在未开启Checkpoint机制的情况下，默认的策略为 "故障不重启策略"
         在开启了Checkpoint机制的情况下，默认的策略为 "固定延迟重启策略",重启间隔1s，最大重启次数为 Integer.MAX_VALUE

    >> 1.故障不重启策略(不推荐): 作业发生故障不会重启
        # 在flink-conf.yaml中设置
        restart-strategy: none

        # 在Flink作业代码中设置，代码中的设置优先级高于flink.conf.yaml
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategy.noRestart());
    >> 2.固定延迟重启策略
        两个参数，一个用于指定尝试重启的最大次数， 一个用于指定两次重启之间要等待的固定时长
        restart-strategy: fixed-delay  # 指定策略为固定延迟重启策略
        restart-strategy.fixed-delay.attempts: 3  #指定重启的最大次数，默认值为1
        restart-strategy.fixed-delay.delay: 10s  # 指定两次重启之间等待的时间间隔，默认值为1

        在Flink作业代码中设置，代码中的设置优先级高于flink-conf.yaml中的设置
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)));

    >> 3.指数延迟重启策略
        使用指数延迟重启策略时，作业故障后会尝试无限次重启作业，两次重启之间的时间间隔将以指数级增长
        restart-strategy: exponential-delay # 指定指数级延迟重启策略
        restart-strategy.exponential-delay.initial-backoff: 10s  # 指定两次重启之间时间间隔的初始值，默认值1s
        restart-strategy.exponential-delay.max-backoff: 2min  # 指定两次重启之间时间间隔的最大值，默认值5min
        restart-strategy.exponential-delay.backoff-multiplier: 2.0  # 指定两次重启之间时间间隔增长的指数，默认值2.0
        restart-strategy.exponential-delay.reset-backoff-threshold: 10min  # 指定Flink作业重新运行多长时间将两次重启的间隔时间恢复到初始值
        restart-strategy.exponential-delay.jitter-factor: 0.1 # 指定重启间隔时间的最大抖动值(加/减一个随机数)，避免作业同时失败后又在统一时刻重启对Flink依赖的其他组件产生影响

        在Flink作业代码中设置，代码中的优先级高于flink-conf.yaml中的设置
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.exponentialDelayRestart(
             Time.of(10, TimeUnit.SECONDS),     # 指定两次重启之间时间间隔的初始值
             Time.of(2, TimeUnit.MINUTES),      # 指定两次重启之间时间间隔的最大值
             2.0d,                              # 指定两次重启之间时间间隔增长的指数
             Time.of(10, TimeUnit.MINUTES),     # 指定Flink作业重新运行多长时间将两次重启的间隔时间恢复到初始值
             0.1d                               # 指定重启间隔时间的最大抖动值
        ))

    >> 4.故障率重启策略
        三个参数, 第一个参数用于指定计算故障率的时间间隔、第二个参数用于指定在这个时间间隔中允许失败的最大次数，第三个参数用于指定两次重启间等待的时间间隔
        > 在flink-conf.yaml中设置
        restart-strategy: failure-rate # 指定故障率重启策略
        restart-strategy.failure-rate.failure-rate-interval: 5min # 第一个参数用于指定计算故障率的时间间隔
        restart-strategy.failure-rate.max-failures-per-interval: 3 # 第二个参数用于指定在这个时间间隔中允许失败的最大次数，默认值为1
        restart-strategy.failure-rate.delay: 10s # 第三个参数用于指定两次重启间等待的时间间隔,默认1s
        > 在Flink作业代码中设置
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
            3,                              // 指定在这个时间间隔中允许失败的最大次数
            Time.of(5, TimeUnit.MINUTES),  // 指定用于计算故障率的时间间隔
            Time.of(10, TimeUnit.SECONDS) // 指定两次重启之间等待的固定长度时间
        ));

 */
public class RocksDBBackEndExample {
    static {
        // RocksDB由C++编写，在Java中使用需要先加载Native库
        RocksDB.loadLibrary();
    }

    public static void main(String[] args) throws RocksDBException {




        // 第一步，打开数据库
        // 创建数据库配置
        Options dbOpt = new Options();

        // 如果数据库不存在，则自动创建
        dbOpt.setCreateIfMissing(true);
        // 打开数据库,RocksDB默认将数据保存在本地，需要指定数据存储目录
        RocksDB rdb = RocksDB.open(dbOpt, "/Users/wudy.yu/RocksDB/data");

        // 第二步 写入数据
        // RocksDB是以字节数组的方式写入数据库的，我们需要先讲字符串转换为字节数组再写入
        byte[] key = "My Flink".getBytes(StandardCharsets.UTF_8);
        byte[] value = "My RocksDB".getBytes(StandardCharsets.UTF_8);
        // 使用put方法写入
        rdb.put(key, value);

        // 第三步 使用get()方法读取数据
        String readValue = new String(rdb.get(key));
        System.out.println("Read Value From RocksDB=" + readValue);

        // 第4步  使用delete删除数据
        rdb.delete(key);

        // 第5步 再次读取数据
        System.out.println("After Delete,Read Value From RocksDB=" + Arrays.toString(rdb.get(key)));

        // 第6步 关闭RocksDB
        rdb.close();
        dbOpt.close();

    }
}
