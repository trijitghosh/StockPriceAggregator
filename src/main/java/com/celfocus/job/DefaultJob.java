package com.celfocus.job;

import com.celfocus.job.task.Task;
import java.util.Properties;


import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultJob implements Job {

  Task task;
  Properties properties;
  KafkaStreams kafkaStreams;
  private static final Logger log = LoggerFactory.getLogger(DefaultJob.class);
  final StreamsBuilder builder = new StreamsBuilder();

  public DefaultJob(Task task, Properties properties) {
    this.task = task;
    this.properties = properties;
  }

  public void launch() {
    log.info("Initializing task");
    task.init(this.properties, this.builder);

    log.info("Running task");
    task.run();

    this.kafkaStreams = new KafkaStreams(builder.build(), properties);
    kafkaStreams.start();
    Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
  }
}
