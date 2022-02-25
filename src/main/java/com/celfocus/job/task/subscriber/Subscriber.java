package com.celfocus.job.task.subscriber;

import java.util.Properties;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

public interface Subscriber {
  public void init(Properties properties, StreamsBuilder builder);

  public KStream subscribe();
}
