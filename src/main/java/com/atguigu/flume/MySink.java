package com.atguigu.flume;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySink extends AbstractSink implements Configurable {

    private String prefix;
    private String suffix;

    private static final Logger LOG = LoggerFactory.getLogger(AbstractSink.class);

    public Status process() throws EventDeliveryException {
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();

        transaction.begin();

        Status status;

        try {
            Event event = null;

            while ((event = channel.take()) == null) {
                Thread.sleep(500);
            }

            byte[] body = event.getBody();

            String s = new String(body);

            LOG.info(prefix + s + suffix);

            status = Status.READY;
            transaction.commit();
        } catch (Exception e) {

            status = Status.BACKOFF;
            transaction.rollback();
        } finally {

            transaction.close();
        }

        return status;
    }

    public void configure(Context context) {

        prefix = context.getString("prefix", "PD");
        suffix = context.getString("suffix", "SD");
    }
}
