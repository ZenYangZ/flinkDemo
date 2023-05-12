package com.yjx.flink.lambda;

import java.util.Arrays;

/**
 * @ClassName : LambdaTest03
 * @Description : LambdaTest03
 * @Author : YangJiuZhou
 * @Date: 2023-05-11 17:03
 */
public class LambdaTest03 {
    public static void main(String[] args) {
        String[] array = new String[] { "Apple", "Orange", "Banana", "Lemon" };
//                         类       方法名
        Arrays.sort(array, String::compareTo);
        System.out.println(String.join(", ", array));
    }
}
