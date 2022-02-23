package com.celfocus.job.task;

import org.apache.kafka.streams.StreamsBuilder;

import java.util.Properties;

public interface Task {
    public void init(Properties properties, StreamsBuilder builder);
    public void run();
}
