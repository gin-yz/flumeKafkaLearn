package com.cjs.flumeKafkaLearn.flumeLearn;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

public class MySource extends AbstractSource implements Configurable, PollableSource {

    private long delay ;
    private String prefix;
    @Override
    public Status process() throws EventDeliveryException {

        try {
            //创建事件头信息
            HashMap<String, String> hearderMap = new HashMap<>();
            //创建事件
            SimpleEvent event = new SimpleEvent();
            //循环封装事件
            for (int i = 0; i < 5; i++) {
                //给事件设置头信息
                event.setHeaders(hearderMap);
                //给事件设置内容
                event.setBody((prefix + i).getBytes());
                //将事件写入 channel
                getChannelProcessor().processEvent(event);
                TimeUnit.SECONDS.sleep(delay);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return Status.BACKOFF;
        }
        return Status.READY;

    }

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }

    /**
     * delay和prefix都是在配置信息里面定义的,会首先加载这个
     * @param context
     */
    @Override
    public void configure(Context context) {
        delay = context.getLong("delay");
        prefix = context.getString("prefix","cjsdsgdsg");
    }
}
