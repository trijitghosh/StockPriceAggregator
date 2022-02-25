package com.celfocus.job.task.publisher;

import com.iot.ngm.stocks.dtos.AggregatedStock;
import java.util.Properties;
import org.apache.kafka.streams.kstream.KStream;

public interface Publisher {
  public void init(Properties properties);

  public void publish(KStream<String, AggregatedStock> streams);
}
