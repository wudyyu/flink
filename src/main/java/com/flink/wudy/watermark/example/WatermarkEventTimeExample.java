package com.flink.wudy.watermark.example;

/**
 * 1.水位线watermark是什么?
 * > Watermark是一个单位为ms的Unix时间戳，可以在上下游算子件传递，Flink通过Watermark保证各个SubTask之间的事件时间时钟是同步的
 * 2.如何生成waermark?
 *   用户通过代码指定watermark值取自数据中的时间字段，例如create_time, Flink提供两种方法获取数据的时间戳
 *  2.1> DataStream的assignTimestampsAndWatermarks(WatermarkStrategy<T> watermarkStrategy) 方法从数据中获取数据的时间
 *  2.2> 在SourceFunction中获取数据的时间戳并生成Watermark
 *
 * 3.Watermark传输策略
 * 假设一个逻辑数据流： Source -> Filter -> KeyBy/Window -> Sink
 * 分场景讨论：
 * > 3.1 在不同的数据传入策略下，Watermark是如何传输的?
 *  >> 上游算子的SubTask会向下游算子的SubTask广播Watermark
 * > 3.2 数据处理算子是如何处理Watermark的？ 比如Filter算子会将Watermark过滤吗？
 *  >> Watermark的传输不会受到数据处理算子的影响，Watermark中只包含用于标记事件时间进度的时间戳，数据处理算子并不会影响Watermark的传输。
 *     所以经过Filter算子处理后，只有数据过滤了，Watermark依然存在
 *  >> 算子收到Watermark后，会在这条Watermark引起的计算完成后继续向下游传输Watermark
 *
 * > 3.3 下游算子(KeyBy/Window)的一个SubTask收到上游算子(Filter)多个SubTask的Watermark时如何处理? 应该以哪个SubTask的Watermark为准维护事件时间钟?
 *  >> 事件时间窗口 是 Watermark 的最主要应用场景
 *  >> 取上游多个SubTask传输的Watermark的最小值，实现木桶效应
 *
 *  > 4.使用Watermark缓解数据乱序问题
 *   >> 4.1 时钟时间由Watermark更新的，我们整体让Watermark减小1min，原本W(02:00)、W(03:00) 变为 W(01:00)、W(03:00)
 *
 *  ｜ 03:00  ｜ 01:33  ｜ 01:28  ｜ 02:15  ｜ 02:03  ｜ 02:00  ｜ 01:55  ｜ 01:45  ｜ 01:13  ｜ 01:10  ｜ 01:10  |
 * W(03:00) W(02:15)  W(02:15)  W(02:15) W(02:03)  W(02:00)  W(01:55)  W(01:45) W(01:13)  W(01:10) W(01:10)
 *
 *  Watermark 整体减小1分钟
 *  ｜ 03:00  ｜ 01:33  ｜ 01:28  ｜ 02:15  ｜ 02:03  ｜ 02:00  ｜ 01:55  ｜ 01:45  ｜ 01:13  ｜ 01:10  ｜ 01:10  |
 * W(02:00) W(01:15)  W(01:15)  W(01:15) W(01:03)  W(01:00)  W(00:55)  W(00:45) W(00:13)  W(00:10) W(00:10)
 *
 * >> 4.2 由于生产环境无法确定数据乱序的最大时间，如果设置Watermark整体减小时间太长（例如：1小时）,则无法做到实时数据
 *
 * > 5.事件时间乱序问题体系化解决方案
 *   事件时间可以客观反应数据发生的时序特征，因此事件时间窗口的应用非常广泛,但也有如下常见问题：
 * >> 5.1 事件时间窗口不触发计算
 *
 * >> 5.2 数据乱序
 *  >>> 5.2.1 导致数据乱序原因
 *      - 原始数据乱序
 *      - Flink事件时间窗口的计算机制。Watermark推进机制导致窗口触发计算并关闭了窗口，无法将乱序数据放入已经关闭的窗口中，所以乱序的数据被丢弃
 *  >>> 5.2.2 数据乱序问题3中处理方法
 *      - 让窗口晚点触发，等等那些乱序的数据
 *      - 窗口随着Watermark的推进正常触发，窗口先不关闭和销毁，让乱序的的数据能够被放进窗口，重新触发计算。 Flink窗口算子提供了AllowLateness（允许延迟）
 *      - 使用Flink窗口算子提供的旁路输出流功能输出乱序数据
 *
 * > 6.生成Watermark 的 API
 *
 *  >> 在数据源算子的SourceFunction中生成
 *  >> 在任意的DataStream上调用assignTimeStampsAndWatermarks()方法生成Watermark
 *     举例： Source -> Filter -> FlatMap -> Map -> KeyBy/EventTime-TumbleWindow(1min) -> Sink
 *     为了能让这个滚动窗口触发计算，只需要在Map算子之后获取数据的时间戳并生成Watermark，就可以驱动KeyBy/EventTime-TumbleWindow(1min) 算子进行计算了
 *     获取数据时间时间戳 和 生成 Watermark的过程不一定非要在Source算子中完成，只要在时间窗口之前完成就可以
 *  >> WatermarkStrategy 包含 createTimeStampAssigner() 和 createWatermarkGenerator()两个方法
 *  >>> TimeStampAssigner<T> createTimeStampAssigner(TimeStampAssignerSupplier.Context) 方法用于构建TimeStampAssigner（时间戳分配器）,用于从数据中获取事件时间戳
 *  >>> WatermarkGenerator<T> createWatermarkGenerator(WatermarkGeneratorSupplier.Context) 方法用于构建WatermarkGenerator（Watermark生成器）， WatermarkGenerator用于给数据流插入Watermark
 *
 * > 7.双流数据事件窗口关联
 *   Flink 提供了双流数据的事件窗口关联操作
 *   >> 7.1 时间窗口关联(Inner Join)
 *      时间窗口关联是Flink在滚动、滑动、绘画等常用时间窗口的基础上提供的两条流窗口的数据关联操作
 *
 *      stream.join(otherStream)
 *            .where(<KeySelector>)
 *            .equalTo(<KeySelector>)
 *            .window(<WindowAssigner>)
 *            .apply(<JoinFunction>);
 *      输出结果是两条流同一个窗口内数据的笛卡尔积（只有当两条流同意时间窗口内都有数据时），当只有一条流的窗口有数据时，不会产生数据
 *
 *                   滚动窗口关联
 * 曝光流        ｜ 6  5 ｜ 4  ｜ 3    ｜ 2  1 ｜
 * 点击流        ｜ 7  5 ｜ 4  ｜ 3  2 ｜      ｜
 * —————————————————————————————————————————————————> 时间
 * 关联结果        (6,7)   (4,4) (3,3)
 *                (6,5)       (3,2)
 *                (5,7)
 *                (5,5)
 *
 *
 *                   滑动窗口关联
 * 曝光流        ｜ 6  ｜ 4  ｜     ｜ 2  1 ｜
 * 点击流        ｜ 5  ｜ 4  ｜ 3  2｜      ｜
 * —————————————————————————————————————————————————> 时间
 * 关联结果         (6,5)  (4,4)  (2,3)
 *                 (6,4)  (4,3)  (2,2)
 *                 (4,5)  (4,2)  (1,3)
 *                 (4,4)         (1,2)
 *
 *  要完成两条数据路的时间窗口关联，需要实现以下5个方法
 *  > join(otherStream):  等价于 Inner Join
 *  > where(<KeySelector>), equalTo(<KeySelector>): 只有相同key、相同时间范围的窗口才会进行关联, 等价于 JOIN...ON...
 *  > window(<WindowAssigner>):指定两条流关联的时间窗口类型, WindowAssigner是窗口分配器,等价于 WHERE date=...
 *  > apply(<JoinFunction>):用于指定关联数据的处理方式,等价于 SELECT tmp1.*,tmp2.*
 *
 *   >> 7.2 时间区间关联（只支持Inner Join）
 *      时间窗口关联操作存在一个缺点：只有相同时间窗口内的数据才能进行关联
 *      实际生产环境中可能的情况为：
 *          商品在08:59:55曝光给用户  [08:59:00, 09:00:00]
 *          用户在09:00:05、09:00:35 分别点击一次商品  [09:00:00, 09:01:00]
 *      曝光流于点击流不在相同的窗口内
 *    >>> 解决办法： 去掉时间窗口划分的边界，允许曝光流数据关联点击流一段时间范围内的数据
 *
 *    曝光流  ｜08:59:55                 ｜
 *    点击流  ｜    09:00:05     09:00:35｜
 *  —————————｜—————————————————————————｜——————————> 时间
 *         下界08:59:55               上界09:00:35
 *
 * 这种通过一条数据流取关联另一条数据流一段时间内数据的关联操作 叫做 时间区间关联
 * 目前时间区间只支持Inner Join， 并且只支持事件时间(不支持处理时间)
 *
 * 生产环境常见问题解决方案
 * 1.时间时间窗口不触发计算
 * 2.事件时间窗口数据乱序
 * 3.windowAll()方法导致数据倾斜
 *
 * 案例分析：
 * 事件时间为1min的滚动窗口算子Flink作业，逻辑数据流为 Source -> Timestamps/Watermarks -> Filter -> KeyBy/Window -> Sink
 * 作业运行超过30min， 正常消费上游数据源，没有消费延迟，窗口却迟迟不触发计算。或者触发了计算，但是产出结果的事件时间是25min前的，比实际时间慢很多
 * >场景1：没有正确分配Watermark
 *   用户编码问题，根本没有分配Watermark，没有在SourceFunction后者没有使用DataStream的assignTimestampsAndWatermarks()方法获取数据流中的时间戳,也没有分配WaterMark（时间单位：毫秒级Unix时间戳）
 *
 * > 场景2: 分配的WaterMark太少
 *   上游算子数量太少，无法产生足够的Watermark来更新事件时间窗口算子
 *
 * > 场景3:WaterMark 不对齐
 *   SubTask收到多个上游算子的WaterMark时，会取最小值作为当前的事件时间钟，Sub，拖了其他SubTask后腿
 */

public class WatermarkEventTimeExample {

}
