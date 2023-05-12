package com.yjx.operator.customize;

import com.yjx.util.DESUtil;
import org.apache.commons.io.FileUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.File;
import java.util.ArrayList;

/**
 * @ClassName : Flink051016SinkCustomizeRich
 * @Description : Flink051016SinkCustomizeRich
 * @Author : YangJiuZhou
 * @Date: 2023-05-11 10:39
 */
public class Flink051016SinkCustomizeRich {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(4);

        ArrayList<String> list = new ArrayList<>();
        list.add("劝君莫惜金缕衣");
        list.add("劝君须惜少年时");
        list.add("花开堪折直须折");
        list.add("莫待无花空折枝");
        list.add("金缕衣, 唐.无名氏");
        DataStreamSource<String> source = executionEnvironment.fromCollection(list);

        source.addSink(new YjxCustomizeRich("data/sink" + System.currentTimeMillis()));

        executionEnvironment.execute();
    }
}

class YjxCustomizeRich extends RichSinkFunction<String>{

    private File file;

    private String filePath;

    private int indexOfThisSubtask;

    private int numberOfParallelSubtasks;

    public YjxCustomizeRich(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
//       获取当前任务的并行度
        this.numberOfParallelSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
//         获取当前子任务的索引
        this.indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
//        获取当前的写出路径
//        1.直接添加this.filePath + File.separator 会产生一个文件
//        this.file = new File(this.filePath + File.separator);
//        2.添加this.filePath + File.separator 后面再接值，比如 + indexOfThisSubtask或其他的值，会产生一个子文件夹，
        this.file = new File(this.filePath + File.separator + indexOfThisSubtask);
//        3.在 File.separator 之后再添加值，然后再重复添加File。separator 或生成更细化的子文件夹
//        this.file = new File(this.filePath + File.separator + indexOfThisSubtask + File.separator + (int)(Math.random()*1000));

    }
    @Override
    public void invoke(String line, Context context) throws Exception {
//        加密数据
        String poem = DESUtil.encrypt("msb230511x", line) + "\r\n";
//        写出数据
        FileUtils.writeStringToFile(file, poem, "utf-8", true);

    }
}
