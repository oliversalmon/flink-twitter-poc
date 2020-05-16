package org.example.streaming.util;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class TimeStampWatermarkGenerator implements AssignerWithPeriodicWatermarks<Tuple4<String,String, String, String>> {

    private final long maxOutOfOrderness = 5500; // 5.5 seconds

    private long currentMaxTimestamp;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(Tuple4<String, String, String, String> element, long previousElementTimeStamp) {
        long timestamp = System.currentTimeMillis();
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
        return timestamp;
    }
}
