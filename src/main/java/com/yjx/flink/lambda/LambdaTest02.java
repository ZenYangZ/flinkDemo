package com.yjx.flink.lambda;

import java.util.Arrays;

/**
 * @ClassName : LambdaTest02
 * @Description : LambdaTest02
 * @Author : YangJiuZhou
 * @Date: 2023-05-11 16:25
 */
public class LambdaTest02 {
    public static void main(String[] args) {
            String[] array = new String[] { "Apple", "Orange", "Banana", "Lemon" };
            Arrays.sort(array, LambdaTest02::cmp);

        System.out.println(String.join(", ", array));
        }

        static int cmp(String s1, String s2) {
            return s1.compareTo(s2);

    }
}
