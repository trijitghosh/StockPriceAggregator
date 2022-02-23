package com.celfocus.job.task.subscriber;

import com.iot.ngm.stocks.dtos.Stock;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class DefaultSubscriber implements Subscriber {

    final Serde<String> keySpecificAvroSerde = new SpecificAvroSerde();
    final Serde<Stock> valueSpecificAvroSerde = new SpecificAvroSerde();
    Properties properties;
    StreamsBuilder builder;

    public void init(Properties properties, StreamsBuilder builder) {
        this.properties = properties;
        this.builder = builder;
        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url",
                properties.getProperty("schema.registry.url"));

        keySpecificAvroSerde.configure(serdeConfig, true);
        valueSpecificAvroSerde.configure(serdeConfig, false);
    }

    public KStream<String, Stock> subscribe() {

        return this.builder.stream(this.properties.getProperty("in-topic"), Consumed.with(keySpecificAvroSerde,
                valueSpecificAvroSerde));

    }
}
