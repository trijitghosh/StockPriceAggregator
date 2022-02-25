package com.celfocus.job.task.transformer;

import com.iot.ngm.stocks.dtos.AggregatedStock;
import com.iot.ngm.stocks.dtos.Stock;
import org.apache.kafka.streams.kstream.KStream;

public interface Transformer {
  public KStream<String, AggregatedStock> transform(KStream<String, Stock> kStreams);
}
