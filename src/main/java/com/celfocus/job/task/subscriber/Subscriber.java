package com.celfocus.job.task.subscriber;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public interface Subscriber {
    public void init(Properties properties, StreamsBuilder builder);
    public KStream subscribe();
}
