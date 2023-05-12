package com.yjx.flink.lambda;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

/**
 * @ClassName : LambdaTest
 * @Description : LambdaTest
 * @Author : YangJiuZhou
 * @Date: 2023-05-11 16:01
 */
public class LambdaTest {
    public static void main(String[] args) {
        ArrayList<String> strings = new ArrayList<>();
        strings.add("aaaa");
        strings.add("aba");
        strings.add("aca");

//        Arrays.sort(strings, new Comparator<String>() {
//            public int compare(String s1, String s2) {
//                return s1.compareTo(s2);
//            }
//        });

        String[] array = new String[] { "Apple", "Orange", "Banana", "Lemon" };
        Arrays.sort(array, (s1, s2) -> {
            return s1.compareTo(s2);
        });
        System.out.println(String.join(", ", array));
    }
}
