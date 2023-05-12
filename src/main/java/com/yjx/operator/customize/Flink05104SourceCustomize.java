package com.yjx.operator.customize;

import com.yjx.util.DESUtil;
import org.apache.commons.io.FileUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.io.File;
import java.util.List;

/**
 * @ClassName : Flink05104SourceCustomize
 * @Description : Flink05104SourceCustomize
 * @Author : YangJiuZhou
 * @Date: 2023-05-10 16:18
 */
public class Flink05104SourceCustomize {
    public static void main(String[] args) throws Exception {
//        执行环境
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

//        获取数据源 ，如果不做设置，默认的并行度是1
        DataStreamSource<String> source = streamExecutionEnvironment.addSource(new YjxCustomizeSource("data/secret.txt")).setParallelism(1);

//        直接转换并打印，默认的并行度是1
        source.print().setParallelism(1);

//        运行环境
        streamExecutionEnvironment.execute();
    }
}

/*自定义数据源
* */

//需要实现ParallelSourceFunction接口
class YjxCustomizeSource implements ParallelSourceFunction<String> {

    private String filePath;

    public YjxCustomizeSource(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
//        读取时数据文件
        List<String> lines = FileUtils.readLines(new File(filePath), "utf-8");
//        遍历数据
        for (String line : lines) {
            ctx.collect(DESUtil.decrypt("msb051016",line));
        }
    }

    @Override
    public void cancel() {

    }
}
