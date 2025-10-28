package com.flink.wudy.demo.connect.source;


import com.flink.wudy.demo.connect.models.ConnectInputModel2;
import com.flink.wudy.demo.flatMap.models.LogModel;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import java.util.Arrays;
import java.util.List;

public class ConnectSource2 implements ParallelSourceFunction<ConnectInputModel2> {
    @Override
    public void run(SourceContext<ConnectInputModel2> sourceContext) throws Exception {

        LogModel logModel1 = new LogModel("page1", "btn2");
        LogModel logModel2 = new LogModel("page2", "btn1");
        LogModel logModel3 = new LogModel("page3", "btn1");
        List<LogModel> logModelList = Arrays.asList(logModel1, logModel2, logModel3);

        ConnectInputModel2 inputModel2 = new ConnectInputModel2("wudy.yu", logModelList);
        sourceContext.collect(inputModel2);

    }

    @Override
    public void cancel() {

    }
}
