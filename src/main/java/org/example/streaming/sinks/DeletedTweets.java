package org.example.streaming.sinks;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.bson.Document;

import java.util.UUID;

public class DeletedTweets extends RichSinkFunction<Tuple2<String, String>>{

    private MongoClient mongoClient;
    private MongoDatabase database;
    private MongoCollection<Document> coll;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        mongoClient = MongoClients.create("mongodb://localhost:27017");
        //mongoClient = MongoClients.create("mongodb://root:kGsNIbV9ZE@flink-mongo-release-mongodb.default.svc.cluster.local:27017");
        database = mongoClient.getDatabase("TWITTER");
        coll = database.getCollection("deleted_tweets");
    }

    @Override
    public void close() throws Exception {
        super.close();
        mongoClient.close();
    }

    @Override
    public void invoke(Tuple2<String, String> value, Context context) throws Exception {
        coll.insertOne(Document.parse(value.f1).append("_id", UUID.randomUUID().toString()));
    }
}
