package com.atguigu.flume;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;
import java.util.Map;

public class MyInterceptor implements Interceptor {


    public void initialize() {

    }

    public Event intercept(Event event) {

        byte[] body = event.getBody();
        Map<String, String> headers = event.getHeaders();

        if (body.length >= 1) {
            if ((body[0] <= 'z' && body[0] >= 'a') || (body[0] <= 'Z' && body[0] >= 'A')) {
                headers.put("type", "letter");
            } else {
                headers.put("type", "notLetter");
            }
        }

        return event;
    }

    public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
            intercept(event);
        }

        return events;
    }

    public void close() {

    }

    public static class MyBuilder implements Interceptor.Builder{

        public Interceptor build() {
            return new MyInterceptor();
        }

        public void configure(Context context) {

        }
    }
}
