package com.celfocus.job.task.publisher;

import com.iot.ngm.stocks.dtos.AggregatedStock;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public interface Publisher {
    public void init(Properties properties);
    public void publish(KStream<String, AggregatedStock> streams);
}
