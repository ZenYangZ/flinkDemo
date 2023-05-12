package com.yjx.flink;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @ClassName : Hello08TypeInformationCreate
 * @Description : Hello08TypeInformationCreate
 * @Author : YangJiuZhou
 * @Date: 2023-05-09 15:49
 */
public class Hello08TypeInformationCreate {
//    解决Hello06类型推断问题
    public static void main(String[] args) {
//    第一种方式
        TypeInformation<String> of = TypeInformation.of(String.class);

//     第二种方式
        TypeInformation<Tuple2<String, Long>> info = TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
        });

//      第三种方式
        TypeInformation<String> string = Types.STRING;
        TypeInformation<Tuple> tuple = Types.TUPLE(Types.STRING, Types.LONG);

    }
}
