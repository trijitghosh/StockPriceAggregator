package com.celfocus.job.task.functions;

import com.iot.ngm.stocks.dtos.Stock;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class MyEventTimeExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord, long l) {
        Stock myPojo = (Stock) consumerRecord.value();
        return myPojo.getTime().toEpochMilli();
    }
}
