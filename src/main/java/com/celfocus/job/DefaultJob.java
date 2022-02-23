package com.celfocus.job;

import com.celfocus.job.task.Task;
import org.apache.kafka.streams.StreamsBuilder;

import java.util.Properties;

public class DefaultJob implements Job{

    Task task;
    Properties properties;
    StreamsBuilder builder;

    DefaultJob(Task task, Properties properties, StreamsBuilder builder){
        this.task = task;
        this.properties = properties;
        this.builder = builder;
    }

    public void launch() {
        task.init(this.properties, this.builder);
        task.run();
    }
}
