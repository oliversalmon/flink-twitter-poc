package org.example.streaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import  org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

public class TweetTypeIdentifer implements MapFunction<String, Tuple2<String, String>> {

    private volatile ObjectMapper mapper;


    public Tuple2<String, String> map(String rawTweet) throws Exception {
        return parse(rawTweet);
    }

    private Tuple2<String, String> parse(String rawTweet) throws Exception {
        String tweetType = "UNKNOWN";
        Tuple2<String, String> result = new Tuple2<>();
        result.f1 = rawTweet;
        mapper = new ObjectMapper();
        final ObjectNode node = new ObjectMapper().readValue(rawTweet, ObjectNode.class);
        if(node.has("created_at")){
            tweetType = "CREATED";
        }
        else if(node.has("delete")){
            tweetType = "DELETED";
        }
        else{
            tweetType = "UNKNOWN";
        }

        result.f0 = tweetType;
        return result;
    }
}
