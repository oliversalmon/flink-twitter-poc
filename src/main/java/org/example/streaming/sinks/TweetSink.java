package org.example.streaming.sinks;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import com.mongodb.client.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ServerAddress;
import com.mongodb.MongoCredential;
import com.mongodb.MongoClientOptions;
import org.bson.Document;

import java.util.Arrays;
import java.util.UUID;

public class TweetSink extends RichSinkFunction<Tuple4<String, String, String, String>> {

    private MongoClient mongoClient;
    private MongoDatabase database;
    private MongoCollection<Document> coll;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        mongoClient = MongoClients.create("mongodb://root:ixzzzweGr9@localhost:27017");
        //mongoClient = MongoClients.create("mongodb://localhost:27017");
        database = mongoClient.getDatabase("TWITTER");
        coll = database.getCollection("tweets");
    }

    @Override
    public void close() throws Exception {
        super.close();
        mongoClient.close();
    }

    @Override
    public void invoke(Tuple4<String, String, String, String> value, Context context) throws Exception {
        coll.insertOne(Document.parse(value.f3).append("_id", UUID.randomUUID().toString()));
    }

}
