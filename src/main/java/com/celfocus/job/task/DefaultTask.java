package com.celfocus.job.task;

import com.celfocus.job.task.publisher.Publisher;
import com.celfocus.job.task.subscriber.Subscriber;
import com.celfocus.job.task.transformer.Transformer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class DefaultTask implements Task{

    Subscriber subscriber;
    Transformer transformer;
    Publisher publisher;
    Properties properties;

    public DefaultTask(Subscriber subscriber, Transformer transformer, Publisher publisher){
        this.subscriber = subscriber;
        this.transformer = transformer;
        this.publisher = publisher;
    }

    @Override
    public void init(Properties properties, StreamsBuilder builder) {
        this.properties = properties;
        subscriber.init(properties, builder);
        publisher.init(properties);
    }

    @Override
    public void run() {
        KStream kStream = subscriber.subscribe();
        KStream aggKStream = transformer.transform(kStream);
        publisher.publish(aggKStream);
    }
}
