package cn.idongjia.flume.test;

import org.apache.commons.collections.functors.ExceptionClosure;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by wuyue on 16/7/14.
 */
public class JsonTest {

    public static void main(String[] args) {
        ObjectMapper mapper = new ObjectMapper();
        List<String> list = new ArrayList<>();
        list.add("wuyue");
        list.add("hello");
        list.add("test");
        String str = "";
        try{
            str = mapper.writeValueAsString(list);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(str);

    }

//    public static void main(String[] args) {
//        List<Event> eventList = new ArrayList<>();
//        String str = "{\n" +
//                "    \"systemVersion\": \"用户系统版本\",\n" +
//                "    \"appVersion\": \"使用的东家app版本\",\n" +
//                "    \"mobile\": \"机型信息\",\n" +
//                "    \"platform\": 1,\n" +
//                "    \"deviceNumber\": \"唯一设备号，ios这边用idfv，android待定，如无法获取则空值\",\n" +
//                "    \"macSerial\": \"78:1F:DB:A7:E9:EE\",\n" +
//                "    \"channel\": \"90000（渠道）\",\n" +
//                "    \"locale\": \"语言\",\n" +
//                "    \"timeZone\": \"时区\",\n" +
//                "    \"resolution\": \"resolution\",\n" +
//                "    \"idfa\": \"ios平台广告设备id\",\n" +
//                "    \"mobileOperator\": \"运营商\",\n" +
//                "    \"appName\": \"mj\",\n" +
//                "    \"events\": [\n" +
//                "        {\n" +
//                "            \"pid\": \"需要记录的编号\",\n" +
//                "            \"additional\": {\n" +
//                "                \"shareType\": \"1微信，2微信朋友圈，3qq，4qq空间，5微博\",\n" +
//                "                \"loginMethod\": \"qq\",\n" +
//                "                \"timsOFVideoPlay\": 10000\n" +
//                "            },\n" +
//                "            \"refererId\": \"来源页面或者操作的编号，启动的时候访问首页，来源页也是首页\",\n" +
//                "            \"eid\": \"click\",\n" +
//                "            \"postion\": \"相对位置\",\n" +
//                "            \"appid\": \"1234567\",\n" +
//                "            \"uid\": \"用户id\",\n" +
//                "            \"network\": \"EDGE\",\n" +
//                "            \"time\": 100\n" +
//                "        }\n" +
//                "    ]\n" +
//                "}";
//        System.out.println(str);
//
//        try {
////        bodyStr = new String(body, "UTF-8");
//            ObjectMapper mapper = new ObjectMapper();
//            Map<?, ?> map = mapper.readValue(str, Map.class);
//            List<Map<String, String>> events = (List<Map<String, String>>) map.get("events");
//            Map<String, Object> publicMap = (Map<String, Object>)map;
//            publicMap.remove("events");
//            events.stream().forEach(ev -> {
//                SimpleEvent simpleEvent = new SimpleEvent();
////                simpleEvent.setHeaders(headers);
//                publicMap.remove("event");
//                publicMap.put("event", ev);
//                try {
//                    simpleEvent.setBody(mapper.writeValueAsBytes(publicMap));
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//                eventList.add(simpleEvent);
//            });
//            System.out.println("1");
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        System.out.println("1");
//    }

}
