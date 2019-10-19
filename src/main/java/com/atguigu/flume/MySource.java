package com.atguigu.flume;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

import java.util.HashMap;

public class MySource extends AbstractSource implements Configurable,PollableSource {

    private String prefix;
    private String suffix;

    private Long delay;

    private  int n;

    public Status process() throws EventDeliveryException {

        ChannelProcessor channelProcessor = getChannelProcessor();
        Status status;
        try {
            for (int i =0;i < n;i++) {
                Event event = new SimpleEvent();
                event.setBody((prefix + i + suffix).getBytes());
                event.setHeaders(new HashMap<String,String>());
                channelProcessor.processEvent(event);
                Thread.sleep(delay);
            }
            status = Status.READY;
        } catch (Exception e) {
            status = Status.BACKOFF;
        }
        return status;
    }

    public long getBackOffSleepIncrement() {
        return 0;
    }

    public long getMaxBackOffSleepInterval() {
        return 0;
    }

    public void configure(Context context) {
        prefix = context.getString("prefix","Default");
        prefix = context.getString("suffix","SDefault");

        delay = context.getLong("delay",2000L);
        n = context.getInteger("count",5);
    }
}
