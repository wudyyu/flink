package com.flink.wudy.window.trigger.example;


import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

/**
 * 自定义EarlyFireEventTimeTrigger触发器
 */
public class EarlyFireEventTimeTrigger<W extends Window> extends Trigger<Object, W> {
    // 提前触发的时间间隔
    private final long interval;

    // 将下一次要触发的时间戳记录在状态中
    private final ReducingStateDescriptor<Long> stateDesc = new ReducingStateDescriptor<Long>(
            "early-fire-time", new EarlyFireEventTimeTrigger.Min(), LongSerializer.INSTANCE
    );

    public EarlyFireEventTimeTrigger(long interval) {
        this.interval = interval;
    }

    public static <W extends Window> EarlyFireEventTimeTrigger<W> of(Time interval){
        return new EarlyFireEventTimeTrigger<>(interval.toMilliseconds());
    }

    @Override
    public boolean canMerge(){
        return Boolean.TRUE;
    }

    //合并窗口进行合并时调用
    @Override
    public void onMerge(W window, OnMergeContext ctx) throws Exception{
        // 合并多个窗口中记录的提前触发计算的时间戳，并将新的时间戳注册到时间时间定时器中
        ctx.mergePartitionedState(stateDesc);
        Long earlyFireTimestamp = ctx.getPartitionedState(stateDesc).get();
        if (earlyFireTimestamp != null){
            ctx.registerEventTimeTimer(earlyFireTimestamp);
        }
    }

    /**
     * 时间窗口算子每输入一条数据都会调用该方法，
     */
    @Override
    public TriggerResult onElement(Object element, long timestamp, W window, TriggerContext ctx) throws Exception {
        // 1.如果Watermark大于窗口最大时间戳，则直接触发计算，否则注册定时器
        if (window.maxTimestamp() <= ctx.getCurrentWatermark()){
            return TriggerResult.FIRE;
        } else {
            ctx.registerEventTimeTimer(window.maxTimestamp());
        }

        //2.注册提前触发的定时器
         //获取下一次要提前触发计算的时间
        ReducingState<Long> earlyFireTimestampState = ctx.getPartitionedState(stateDesc);
        // 如果为空，则按照时间间隔计算下一次要提前触发的时间，并注册定时器
        if (earlyFireTimestampState.get() == null){
            registerEarlyFireTimeStamp(timestamp - (timestamp % interval), window, ctx, earlyFireTimestampState);
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, W window, TriggerContext triggerContext) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception {
        // 如果时间是窗口最大时间戳，则返回FIRE，触发窗口计算
        if (time == window.maxTimestamp()){
            return TriggerResult.FIRE;
        }
        // 如果时间不是窗口最大时间戳，则判断是否是窗口提前触发的时间
        ReducingState<Long> earlyFireTimestampState = ctx.getPartitionedState(stateDesc);
        Long earlyFireTimestamp = earlyFireTimestampState.get();
        //如果定时器触发的时间戳和earlyFireTimestamp 相等，则注册下一个提前触发的定时器，并返回FIRE触发窗口计算
        if (earlyFireTimestamp != null && earlyFireTimestamp == time){
            earlyFireTimestampState.clear();
            registerEarlyFireTimeStamp(time, window, ctx, earlyFireTimestampState);
            return TriggerResult.FIRE;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(W window, TriggerContext ctx) throws Exception {
        ReducingState<Long> earlyFireTimestampState = ctx.getPartitionedState(stateDesc);
        Long earlyFireTimestamp = earlyFireTimestampState.get();
        if (null != earlyFireTimestamp){
            ctx.deleteEventTimeTimer(earlyFireTimestamp);
            earlyFireTimestampState.clear();
        }
    }


    private static class Min implements ReduceFunction<Long> {
        private static final long serialVersionUID = 1L;

        @Override
        public Long reduce(Long value1, Long value2) throws Exception {
            return Math.min(value1, value2);
        }
    }

    private void registerEarlyFireTimeStamp(long time, W window, TriggerContext ctx, ReducingState<Long> earlyFireTimestampState) throws Exception{
        // 下一次要触发的时间戳肯定是窗口内小于等于窗口最大时间戳的一个时间
        long earlyFireTimestamp = Math.min(time+interval, window.maxTimestamp());
        earlyFireTimestampState.add(earlyFireTimestamp);
        ctx.registerEventTimeTimer(earlyFireTimestamp);
    }
}
