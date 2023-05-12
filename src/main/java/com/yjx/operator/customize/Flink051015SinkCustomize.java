package com.yjx.operator.customize;

import com.yjx.util.DESUtil;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.io.File;
import java.util.ArrayList;

/**
 * @ClassName : Flink051015SinkCustomize
 * @Description : Flink051015SinkCustomize
 * @Author : YangJiuZhou
 * @Date: 2023-05-11 09:54
 */
public class Flink051015SinkCustomize {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(2);

        ArrayList<String> list = new ArrayList<>();
        list.add("劝君莫惜金缕衣");
        list.add("劝君须惜少年时");
        list.add("花开堪折直须折");
        list.add("莫待无花空折枝");
        list.add("金缕衣, 唐.无名氏");
        DataStreamSource<String> source = executionEnvironment.fromCollection(list);

        source.addSink(new YjxCustomizeSink("data/sink" + System.currentTimeMillis())).setParallelism(1);

        executionEnvironment.execute();
    }
}

class YjxCustomizeSink implements SinkFunction<String> {

    private File file;

    //    根据传入的字符串创建一个file对象
    public YjxCustomizeSink(String file) {
        this.file = new File(file + File.separator + Math.random()*100 + File.separator + Math.random()*100);
    }

    @Override
    public void invoke(String line, Context context) throws Exception {
//        加密数据
        String poem = DESUtil.encrypt("msb230511x", line) + "\r\n";
//        写出数据
//        if true, then the String will be added to the end of the file rather than overwriting
//        true表示文件可追加，而不是覆写文件
        FileUtils.writeStringToFile(file, poem, "utf-8", true);

        /*Writes a String to a file creating the file if it does not exist.
        Params: file – the file to write
                data – the content to write to the file
                charsetName – the name of the requested charset, null means platform default
                append – if true, then the String will be added to the end of the file rather than overwriting
        Throws: IOException – in case of an I/O error
                UnsupportedCharsetException – thrown instead of .UnsupportedEncodingException in version 2.2 if the encoding is not supported by the VM*/
    }

    @Override
    public void writeWatermark(Watermark watermark) throws Exception {
        SinkFunction.super.writeWatermark(watermark);
    }

    @Override
    public void finish() throws Exception {
        SinkFunction.super.finish();
    }
}