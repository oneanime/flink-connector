package com.fs.utils;

import com.fs.RestapiSource;

import com.fs.bean.Student;
import com.fs.bean.UserActionInfo;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;
import java.util.logging.SimpleFormatter;

public class TestSource {
    public static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    public static SingleOutputStreamOperator restSource() {
        String url = "https://api.virapi.com/vir_gitee3b77b240b7146/useractionlog/flink_test";
        String header = "{'app-token': '$2a$10$W/mKbzvGuYZ6CUmY35lFne0YES2me1W6CBl5LuVoyOSGnPNsEDGIO'}";
        DataStreamSource<String> source = env.addSource(new RestapiSource(url, "GET", header, null, 1000L));
        return source.map(data -> {
            Gson gson = new Gson();
            JsonElement element = JsonParser.parseString(data).getAsJsonObject().get("data");
            String time = element.getAsJsonObject().get("time").getAsString();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            long ts = sdf.parse(time).getTime();
            UserActionInfo userActionInfo = gson.fromJson(element, UserActionInfo.class);
            userActionInfo.setTimestamp(ts);
            return userActionInfo;
        });
    }


    public static DataStreamSource studSource() {
        DataStreamSource<Student> source = env.fromElements(new Student(1, 1), new Student(2, 2));
        return source;
    }

}
