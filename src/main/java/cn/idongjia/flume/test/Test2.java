package cn.idongjia.flume.test;

import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by wuyue on 16/8/5.
 */
public class Test2 {

    public static void main(String[] args) {

//        //查找以Java开头,任意结尾的字符串
//        Pattern pattern = Pattern.compile("\\\\");
//        Matcher matcher = pattern.matcher("Java\\");
//        boolean b= matcher.matches();
//        //当条件满足时，将返回true，否则返回false
//        System.out.println(b);
//        String str = "\\x22hello\\x22";
//        String s = str.replaceAll(Pattern.quote("\\x22"),"\"");
//        System.out.println();

        try {
            String encoding="UTF-8";
            File file = new File("/Users/wuyue/Documents/flume-plugin/src/main/resource/hello.log");
            if(file.isFile() && file.exists()){ //判断文件是否存在
                InputStreamReader read = new InputStreamReader(
                        new FileInputStream(file),encoding);//考虑到编码格式
                BufferedReader bufferedReader = new BufferedReader(read);
                String lineTxt = null;
                while((lineTxt = bufferedReader.readLine()) != null){
                    ObjectMapper mapper = new ObjectMapper();




//                    result = lineTxt.replaceAll(Pattern.quote("\\x22"),"\"");
//                    result = result.replaceAll(Pattern.quote("\\x5C"),"");
//                    result = result.replaceAll(Pattern.quote("\\"),"\\\\");
//                    mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true) ;
////                    LOGGER.info(result);
//                    Map<?, ?> map = mapper.readValue(result, Map.class);
//                    List<Map<String, Object>> events = new ArrayList<>();
//                    events = (List<Map<String, Object>>) map.get("events");
//                    Map<String, Object> publicMap = (Map<String, Object>)map;
//                    System.out.println(result);
                }
                read.close();
            }else{
                System.out.println("找不到指定的文件");
            }
        } catch (Exception e) {
            System.out.println("读取文件内容出错");
            e.printStackTrace();
        }

    }
}
