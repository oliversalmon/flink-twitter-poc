package org.example.streaming.functions;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;

public class UserTweetCounter implements AggregateFunction<Tuple4<String, String, String, String>, Tuple2<String, Long>, String> {

    @Override
    public Tuple2<String, Long> createAccumulator() {
        return new Tuple2<>("",0L);
    }

    @Override
    public Tuple2<String, Long> add(Tuple4<String, String, String, String> record, Tuple2<String, Long> accumulator) {
        return new Tuple2<>(accumulator.f0 = record.f0, accumulator.f1 + 1L);
    }

    @Override
    public String getResult(Tuple2<String, Long> accumulator) {
        return "{\"user\": \""+accumulator.f0+"\", \"numberOfTweetsToday\": "+ accumulator.f1 + "}";
    }

    @Override
    public Tuple2<String, Long> merge(Tuple2<String, Long> a, Tuple2<String, Long> b) {
        return new Tuple2<>(a.f0, a.f1+b.f1);
    }
}
