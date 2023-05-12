package com.yjx.flink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;
import java.util.Objects;

/**
 * @ClassName : Hello07TypeInformationPojo
 * @Description : Hello07TypeInformationPojo
 * @Author : YangJiuZhou
 * @Date: 2023-05-09 15:48
 */
public class Hello07TypeInformationPojo {
    public static void main(String[] args) throws Exception {
//        获取环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

//        1.Source admin-123456 [用户名] - [密码]
        DataStream<String> source = environment.socketTextStream("localhost", 19666);

//        2.Transformation + sink
        source.map(line -> new User((int) (Math.random() * 1000 + 500), line.split("-")[0], line.split("-")[1])).print();

//        3.sink
        environment.execute();
    }


}

class User implements Serializable {
    private Integer id;
    private String username;
    private String password;

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        User user = (User) o;
        return Objects.equals(id, user.id) && Objects.equals(username, user.username) && Objects.equals(password, user.password);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, username, password);
    }

    @Override
    public String toString() {
        return "User{" +
                "id=" + id +
                ", username='" + username + '\'' +
                ", password='" + password + '\'' +
                '}';
    }

    public User() {
    }

    public User(Integer id, String username, String password) {
        this.id = id;
        this.username = username;
        this.password = password;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}