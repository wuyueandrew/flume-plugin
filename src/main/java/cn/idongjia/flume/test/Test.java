package cn.idongjia.flume.test;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by wuyue on 16/7/26.
 */
public class Test {

    public static void main(String[] args){

        Map<String, Integer> m1 = new HashMap<>();
        Map<String, Integer> m2 = new HashMap<>();
        Map<String, Integer> m3 = new HashMap<>();

        m1.put("1", 1);
        m1.put("2", 2);
        m1.put("3", 3);

        m2.put("4", 4);
        m2.put("5", 5);
        m2.put("6", 6);

        m3.put("4", 4);
        m3.put("5", 5);
        m3.put("6", 6);

        m1.putAll(m2);
        m1.putAll(m2);

        System.out.println(m1.toString());
    }
}
