package com.yjx.operator.common;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @ClassName : Flink0510LocalEnvironment
 * @Description : Flink0510LocalEnvironment
 * @Author : YangJiuZhou
 * @Date: 2023-05-10 14:48
 */
public class Flink05101LocalEnvironment {
    public static void main(String[] args) throws Exception {
//        批处理
//        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
//        流处理(事实计算
//        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

//        流失处理本地模式
        Configuration configuration = new Configuration();
        configuration.setString(WebOptions.LOG_PATH,"tmp/log/job.log");
        configuration.setString(ConfigConstants.TASK_MANAGER_LOG_PATH_KEY,"tmp/log/job.log");
//        这个是创建了本地的流式处理环境但是没有webui界面
//        StreamExecutionEnvironment.createLocalEnvironment(configuration);

//        创建了本地的流式处理环境并且有webui界面
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

//        编写代码
        environment.socketTextStream("localhost",19999).map(word -> word.toUpperCase()).print();

//        运行环境
        environment.execute();
    }
}
