package com.flink.wudy.demo.connect.source;

import com.flink.wudy.demo.connect.models.ConnectInputModel1;
import com.flink.wudy.demo.flatMap.models.LogModel;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Arrays;
import java.util.List;

public class ConnectSource1 implements ParallelSourceFunction<ConnectInputModel1> {

    @Override
    public void run(SourceContext<ConnectInputModel1> sourceContext) throws Exception {
        ConnectInputModel1  inputModel1 = new ConnectInputModel1("wudy.yu", new LogModel("page1", "button1"));
        ConnectInputModel1  inputModel2 = new ConnectInputModel1("peter.li", new LogModel("page1", "button1"));

        List<ConnectInputModel1> inputModelList = Arrays.asList(inputModel1, inputModel2);

        for (ConnectInputModel1 inputModel : inputModelList) {
            sourceContext.collect(inputModel);
        }
    }

    @Override
    public void cancel() {

    }
}
