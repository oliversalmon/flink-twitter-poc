/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.example.streaming;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;

import java.util.Properties;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.getConfig().setAutoWatermarkInterval(1000);
		env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
		// Set up Twitter as the data source
		Properties props = new Properties();
		props.setProperty(TwitterSource.CONSUMER_KEY, "lDSDKAh07M5HAktpZIQkwi0Nn");
		props.setProperty(TwitterSource.CONSUMER_SECRET, "Zh3CldrxP8IbEDeTYTSlGghwgtoLwXCRqCLjWY6KvIqgvrJhFl");
		props.setProperty(TwitterSource.TOKEN, "468804467-L5O6Mu0PzETEIX8gXDSpvfcqcg07lNOpr413wGQO");
		props.setProperty(TwitterSource.TOKEN_SECRET, "zSISFJMu0NxMJMhXNVmNWxQhSSt1RmM2dHW3pMBlt5pYZ");
		DataStream<String> rawTweets = env.addSource(new TwitterSource(props));

		// Start processing
		SplitStream<Tuple2<String, String>> split = rawTweets
				.map(new TweetTypeIdentifer())
				.split(new TweetTypeSelector());
		DataStream<Tuple4<String,String, String, String>> newTweets = split
				.select("created")
				.map(new TweetUserAndSubjectIdentifier())
				.keyBy(0);
		DataStream<Tuple2<String,String>> deletedTweets = split.select("deleted");
		newTweets.print();
		//deletedTweets.print();





		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}
}
