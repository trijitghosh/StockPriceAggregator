package com.celfocus.job.task;

import java.util.Properties;
import org.apache.kafka.streams.StreamsBuilder;

public interface Task {
  public void init(Properties properties, StreamsBuilder builder);

  public void run();
}
