package com.yjx.flink.lambda;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @ClassName : LambdaDemo01
 * @Description : LambdaDemo01
 * @Author : YangJiuZhou
 * @Date: 2023-05-12 11:50
 */
public class LambdaDemo01 {

    //    Define a functional interface
    @FunctionalInterface
    interface MathOperation {
        int operate(int a, int b);
    }

    public static void main(String[] args) {
        /*
        Use the lambda expression: Now, you can use lambda expressions to implement the functional interface.
        Lambda expressions have a concise syntax and eliminate the need for anonymous inner classes. */
        MathOperation addition = (a, b) -> a + b;
        int result = addition.operate(2, 3);
        System.out.println(result); // Output: 5


        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);


// Filtering
        List<Integer> evenNumbers = numbers.stream()
                .filter(n -> n % 2 == 0)
                .collect(Collectors.toList());
        System.out.println(evenNumbers); // Output: [2, 4, 6, 8, 10]

// Mapping
        List<String> numberStrings = numbers.stream()
                .map(n -> "Number: " + n)
                .collect(Collectors.toList());
        System.out.println(numberStrings); // Output: ["Number: 1", "Number: 2", ...]

// Calculation
        int sum = numbers.stream()
                .reduce(0, (a, b) -> a + b);
        System.out.println(sum); // Output: 55

    }


}
