package com.celfocus.job.task.transformer;

import com.iot.ngm.stocks.dtos.AggregatedStock;
import com.iot.ngm.stocks.dtos.Stock;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;

import java.time.Duration;
import java.time.Instant;

public class DefaultTransformer implements Transformer {

    final Duration ONE_MIN = Duration.ofMinutes(1);
    final Duration FIVE_MIN = Duration.ofMinutes(5);
    final Duration TEN_MIN = Duration.ofMinutes(10);
    final Duration THIRTY_MIN = Duration.ofMinutes(30);
    final Duration SIXTY_MIN = Duration.ofMinutes(60);

    final TimeWindows tumblingWindowOneMin = TimeWindows.ofSizeWithNoGrace(ONE_MIN);
    final TimeWindows tumblingWindowFiveMin = TimeWindows.ofSizeWithNoGrace(FIVE_MIN);
    final TimeWindows tumblingWindowTenMin = TimeWindows.ofSizeWithNoGrace(TEN_MIN);
    final TimeWindows tumblingWindowThirtyMin = TimeWindows.ofSizeWithNoGrace(THIRTY_MIN);
    final TimeWindows tumblingWindowSixtyMin = TimeWindows.ofSizeWithNoGrace(SIXTY_MIN);


    public KStream<String, AggregatedStock> transform(KStream<String, Stock> kStreams) {

        final KStream<String, AggregatedStock> oneMinAggStock = getAggStock(kStreams, tumblingWindowOneMin);
        final KStream<String, AggregatedStock> fiveMinAggStock = getAggStock(kStreams, tumblingWindowFiveMin);
        final KStream<String, AggregatedStock> tenMinAggStock = getAggStock(kStreams, tumblingWindowTenMin);
        final KStream<String, AggregatedStock> thirtyMinAggStock = getAggStock(kStreams, tumblingWindowThirtyMin);
        final KStream<String, AggregatedStock> sixtyMinAggStock = getAggStock(kStreams, tumblingWindowSixtyMin);

        return oneMinAggStock.merge(fiveMinAggStock).merge(tenMinAggStock).merge(thirtyMinAggStock).merge(sixtyMinAggStock);

    }

    private KStream<String, AggregatedStock> getAggStock(KStream<String, Stock> kStreams, TimeWindows window){
        return kStreams.groupByKey().
                windowedBy(window).reduce((v1, v2) -> {
                    Instant t = v1.getTime().compareTo(v2.getTime()) > 0 ? v1.getTime() : v2.getTime();
                    Float m_open = Math.min(v1.getOpen(), v2.getOpen()) ;
                    Float m_close = Math.max(v1.getClose(), v2.getClose());
                    return new Stock(t, m_open, null, null, m_close, v1.getSymbol());
                }).toStream().map((key, stock) ->
                        KeyValue.pair(stock.getSymbol(), new AggregatedStock(stock.getTime(), stock.getOpen(),
                                stock.getClose(), stock.getSymbol()))
                );
    }
}
