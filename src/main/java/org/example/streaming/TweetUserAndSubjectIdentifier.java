package org.example.streaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

public class TweetUserAndSubjectIdentifier implements MapFunction<Tuple2<String, String>, Tuple4<String, String, String, String>> {

    private volatile ObjectMapper mapper;

    public Tuple4<String, String, String, String> map(Tuple2<String, String> tweet) throws Exception {
        Tuple4<String, String, String, String> result = new Tuple4<>();
        result.f3 = tweet.f1;
        mapper = new ObjectMapper();
        JsonNode rootNode = mapper.readTree(tweet.f1);
        result.f0 = rootNode.path("user").path("screen_name").getTextValue();
        result.f2 = rootNode.path("text").getTextValue();
        result.f1 = rootNode.path("id_str").getTextValue();

        return result;


    }
}
