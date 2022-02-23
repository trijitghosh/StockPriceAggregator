package com.celfocus.job.task.publisher;

import com.iot.ngm.stocks.dtos.AggregatedStock;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class DefaultPublisher implements Publisher{

    final Serde<String> keySpecificAvroSerde = new SpecificAvroSerde();
    final Serde<AggregatedStock> valueSpecificAvroSerde = new SpecificAvroSerde();
    Properties properties;


    public void init(Properties properties) {
        this.properties = properties;
        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url",
                properties.getProperty("schema.registry.url"));

        keySpecificAvroSerde.configure(serdeConfig, true);
        valueSpecificAvroSerde.configure(serdeConfig, false);
    }


    public void publish(KStream<String, AggregatedStock> streams) {
        streams.to(properties.getProperty("out-topic"), Produced.with(keySpecificAvroSerde, valueSpecificAvroSerde));
    }

}
