package com.celfocus;

import com.celfocus.job.DefaultJob;
import com.celfocus.job.task.DefaultTask;
import com.celfocus.job.task.functions.MyEventTimeExtractor;
import com.celfocus.job.task.publisher.DefaultPublisher;
import com.celfocus.job.task.subscriber.DefaultSubscriber;
import com.celfocus.job.task.transformer.DefaultTransformer;
import org.apache.kafka.streams.StreamsConfig;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class StockPriceAggregator {

  public static void main(String[] args) throws IOException {

    Properties properties = new Properties();
    properties.load(new FileReader("src/main/resources/config.properties"));
    properties.setProperty(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
            MyEventTimeExtractor.class.getName());
    DefaultSubscriber subscriber = new DefaultSubscriber();
    DefaultTransformer transformer = new DefaultTransformer();
    DefaultPublisher publisher = new DefaultPublisher();

    DefaultTask task = new DefaultTask(subscriber, transformer, publisher);
    DefaultJob job = new DefaultJob(task, properties);
    job.launch();

  }

}
