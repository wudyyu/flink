package com.flink.wudy.valueState.function;

import com.flink.wudy.valueState.model.InputModel;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;

public class ProductKeyedRichMapFunction extends RichMapFunction<InputModel, InputModel> {
    // 定义用于存储每一种商品累计销售额的 ValueStateDescriptor
    private ValueStateDescriptor<Long> cumulateIncomeStateDescriptor = new ValueStateDescriptor<Long>("cumulate-income", Long.class);
    private transient ValueState<Long> cumulateIncomeState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 从RichFunction的上下文获取状态句柄
        this.cumulateIncomeState = this.getRuntimeContext().getState(cumulateIncomeStateDescriptor);
    }

    @Override
    public InputModel map(InputModel inputModel) throws Exception {
        Long cumulateIncome = this.cumulateIncomeState.value();
        if (null == cumulateIncome){
            cumulateIncome = inputModel.getIncome();
        } else {
            cumulateIncome += inputModel.getIncome();
        }

        // 通过ValueState提供的update()方法将商品的累积销售额更新到状态中
        this.cumulateIncomeState.update(cumulateIncome);
        inputModel.setIncome(cumulateIncome);
        return inputModel;
    }
}
