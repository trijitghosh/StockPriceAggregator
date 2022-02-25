package com.celfocus.job.task.publisher;

import com.celfocus.job.DefaultJob;
import com.iot.ngm.stocks.dtos.AggregatedStock;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultPublisher implements Publisher {

  final Serdes.StringSerde keySpecificAvroSerde = new Serdes.StringSerde();
  final Serde<AggregatedStock> valueSpecificAvroSerde = new SpecificAvroSerde();
  Properties properties;
  private static final Logger log = LoggerFactory.getLogger(DefaultPublisher.class);


  public void init(Properties properties) {
    this.properties = properties;
    final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url",
        properties.getProperty("schema.registry.url"));

    //keySpecificAvroSerde.configure(serdeConfig, true);
    valueSpecificAvroSerde.configure(serdeConfig, false);
  }


  public void publish(KStream<String, AggregatedStock> streams) {
    streams.peek((k, stock) -> log.info("Agg record = " + stock)).to(properties.getProperty("out-topic"),
        Produced.with(keySpecificAvroSerde, valueSpecificAvroSerde));
  }

}
