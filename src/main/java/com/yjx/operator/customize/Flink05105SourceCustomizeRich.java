package com.yjx.operator.customize;

import com.yjx.util.DESUtil;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.File;
import java.util.List;

/**
 * @ClassName : Flink05105SourceCustomizeRich
 * @Description : Flink05105SourceCustomizeRich
 * @Author : YangJiuZhou
 * @Date: 2023-05-10 16:54
 */
public class Flink05105SourceCustomizeRich {
    public static void main(String[] args) throws Exception {
        //执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取数据源
        DataStreamSource<String> source = environment.addSource(new YjxxtCustomSourceRich("data/secret.txt")).setParallelism(7);
//        转换数据 + 输出数据
        source.map(new RichMapFunction<String, String>() {

            @Override
            public void setRuntimeContext(RuntimeContext t) {
                super.setRuntimeContext(t);
            }

            @Override
            public RuntimeContext getRuntimeContext() {
                return super.getRuntimeContext();
            }

            @Override
            public IterationRuntimeContext getIterationRuntimeContext() {
                return super.getIterationRuntimeContext();
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                super.close();
            }
//         以上是AbstractRichFunction 的方法  ----------------------------------------------------------------

            //            必须实现的方法
            @Override
            public String map(String value) throws Exception {
                return value.toUpperCase() + ":" + getRuntimeContext().getIndexOfThisSubtask();
            }
        }).setParallelism(2).print();
//         运行环境
        environment.execute();
    }

}


class YjxxtCustomSourceRich extends RichParallelSourceFunction<String> {

    private String filePath;

    private int numberOfParallelSubtasks;

    private int indexOfThisSubtask;

    public YjxxtCustomSourceRich(String filePath) {
        this.filePath = filePath;
    }

//    当这个函数对象被调用的时候，默认执行一次，类似于JUnit的Before注解；在程序执行前进行的一些操作
    @Override
    public void open(Configuration parameters) throws Exception {
        this.numberOfParallelSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
        this.indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
        System.out.println("YjxxtCustomSourceRich.open任务的并行度为[" + numberOfParallelSubtasks + "]当前任务的并行度编号为[" + indexOfThisSubtask + "]");
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
//        读取数据文件
        List<String> lines = FileUtils.readLines(new File(filePath), "utf-8");
//        遍历数据
        for(String line : lines) {
//            0.并行度索引，所有的子任务都会执行一次 （暂定，具体的还没搞清楚)
//            1.numberOfParallelSubtasks是并行的任务数量，为n ，那么对应的并行任务的索引范围为[0,n-1]
//            2.针对同一个字符串，由于string的是固定的，那么hashCode的值也是固定的，那么它除以numberOfParallelSubtasks的余数也是固定的，是在[0,n-1]范围这么一个数
//            3.那么只有恰好算式得出来的余数 和 分配的 index子任务索引相同的时候，才会执行
            if (Math.abs(line.hashCode()) % numberOfParallelSubtasks == indexOfThisSubtask){
               ctx.collect(DESUtil.decrypt("msb05101630",line) + "[index : " +indexOfThisSubtask + "]");
            }
        }
    }

    @Override
    public void cancel() {

    }
}
