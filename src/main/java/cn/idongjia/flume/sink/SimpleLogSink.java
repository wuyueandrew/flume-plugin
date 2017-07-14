package cn.idongjia.flume.sink;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by wuyue on 16/7/14.
 */
public class SimpleLogSink extends AbstractSink implements Configurable {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(SimpleLogSink.class);

    public void configure(Context context) {
        return;
    }

    public Status process() throws EventDeliveryException {
        Status result = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = null;
        Event event = null;
        try {
            long processedEvent = 0;
            transaction = channel.getTransaction();
            transaction.begin();// 事务开始
            for (; processedEvent < 10; processedEvent++) {
                event = channel.take();// 从channel取出一个事件
                if (event == null) {
                    result = Status.BACKOFF;
                    break;
                }
                byte[] eventBody = event.getBody();
                String eventData = new String(eventBody, "UTF-8");
                LOGGER.info(eventData);
                result = Status.READY;
            }
            transaction.commit();
        } catch (java.io.UnsupportedEncodingException e) {
            LOGGER.error(new StringBuilder("编码不支持").append("UTF-8").toString());
            e.printStackTrace();
        } finally {
            if (transaction != null) {
                transaction.close();
            }
        }
        return result;
    }
}
