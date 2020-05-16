package org.example.streaming.util;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;

import java.util.ArrayList;
import java.util.List;

public class TweetTypeSelector implements OutputSelector<Tuple2<String,String>> {
    @Override
    public Iterable<String> select(Tuple2<String, String> tweet) {
        List<String> output = new ArrayList<String>();
        if (tweet.f0.equals("CREATED")) {
            output.add("created");
        }
        else if(tweet.f0.equalsIgnoreCase("DELETED")){
            output.add("deleted");
        }
        else {
            output.add("unknown");
        }
        return output;
    }
}
