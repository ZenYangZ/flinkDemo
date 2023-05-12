package com.yjx.flink.lambda;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * @ClassName : LambdaDemo02
 * @Description : LambdaDemo02
 * @Author : YangJiuZhou
 * @Date: 2023-05-12 11:57
 */
public class LambdaDemo02 {
    public static void main(String[] args) {
//        1.Passing a lambda expression as an argument to a method that expects a functional interface:

        Function<Integer, Integer> increment = (number) -> number + 1;
        int result = increment.apply(5); // result is 6
        System.out.println(result);

//        2.Creating an object that implements a functional interface using a lambda expression and storing it in a variable:
        
        
        Predicate<String> startsWithA = (str) -> str.startsWith("A");
        boolean result_ = startsWithA.test("Apple"); // result is true
        System.out.println(result_);
//        3. Passing a method reference as an argument to a method that expects a functional interface:
        
        
        List<Integer> numbers = Arrays.asList(1, 2, 3);
        numbers.forEach(System.out::println);

//        4.Creating an object that implements a functional interface using a constructor reference and storing it in a variable:
        
        
        Supplier<Date> currentDate = Date::new;
        Date now = currentDate.get(); // current date and time
        System.out.println(now);
//        5.Storing a lambda expression or method reference in a field of a class and using it in an instance method:


    }
    public class Calculator {
        private final BinaryOperator<Integer> addition = (a, b) -> a + b;

        public int add(int a, int b) {
            return addition.apply(a, b);
        }
    }
}
