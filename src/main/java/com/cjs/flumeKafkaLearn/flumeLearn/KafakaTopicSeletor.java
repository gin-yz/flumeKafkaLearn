/*
* 将含有ｈｅｌｌｏ字符串的消息发至ｆｉｒｓｔ　Ｔｏｐｉｃ，不含的发给ｔｈｉｒｄ　Ｔｏｐｉｃ
* */

package com.cjs.flumeKafkaLearn.flumeLearn;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class KafakaTopicSeletor implements Interceptor {
    List<Event> eventList;
    @Override
    public void initialize() {
        this.eventList = new ArrayList<>();
    }

    @Override
    public Event intercept(Event event) {
        Map<String, String> headers = event.getHeaders();

        String body = new String(event.getBody());
        if(body.contains("hello")){
            headers.put("topic","first"); //key一定要为topic,value为topic的名字
        }else headers.put("topic","third"); //value为topic的名字
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        eventList.clear();
        list.forEach(new Consumer<Event>() {
            @Override
            public void accept(Event event) {
                eventList.add(intercept(event));
            }
        });
        return eventList;
    }

    @Override
    public void close() {

    }


    public static class Builder implements Interceptor.Builder{
        @Override
        public Interceptor build() {
            return new TypeSeletor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
