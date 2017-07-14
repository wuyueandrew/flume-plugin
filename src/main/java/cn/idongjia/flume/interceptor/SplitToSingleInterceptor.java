package cn.idongjia.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Created by wuyue on 16/7/14.
 */
public class SplitToSingleInterceptor implements Interceptor {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(SplitToSingleInterceptor.class);

    public SplitToSingleInterceptor(Context ctx) {

    }

    public void initialize() {
        //No op
    }

    public Event intercept(Event event) {
        return null;
    }

    public List<Event> intercept(List<Event> list) {
        List<Event> eventList = new ArrayList<>();

        list.stream().forEach( event -> {
            List<Event> el = singleToList(event);
            el.stream().forEach( e -> {
                eventList.add(e);
            });
        });

        return eventList;
    }

    public List<Event> singleToList(Event event) {
        Event clone =  new SimpleEvent();
        List<Event> eventList = new ArrayList<>();

        //获取Headers
        final Map<String, String> headers = event.getHeaders();

        //获取Body
        byte[] body = event.getBody();
        String bodyStr = null;
        try {
            bodyStr = new String(body, "UTF-8");
            LOGGER.info(bodyStr);
            String result = bodyStr.replaceAll(Pattern.quote("\\x22"), "\"");
            result = result.replaceAll(Pattern.quote("\\x5C"), "");
            event.setBody(result.getBytes());
            eventList.add(event);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return eventList;
    }

    public void close() {
        //No op
    }

    public static class Builder implements Interceptor.Builder {

        private Context ctx;

        public Interceptor build() {
            return new SplitToSingleInterceptor(ctx);
        }

        public void configure(Context context) {
            this.ctx = context;
        }
    }
}
