package com.cjs.flumeKafkaLearn.flumeLearn;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class TypeSeletor implements Interceptor {
    private List<Event> eventList;
    @Override
    public void initialize() {
        eventList = new ArrayList<>();
    }


    @Override
    public Event intercept(Event event) {
        Map<String, String> headers = event.getHeaders();

        String body = new String(event.getBody());
        if(body.contains("hello")){
            headers.put("selfType","hello");
        }else headers.put("selfType","noHello");
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
