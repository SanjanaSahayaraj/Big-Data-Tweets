package storm;

import twitter4j.FilterQuery;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class TwitterTopology {

	 private static String consumerKey = "insert_value";
	 private static String consumerSecret = "insert_value";
	 private static String accessToken = "insert_value";
	 private static String accessTokenSecret = "insert_value";
	public static void main(String args[]) throws InterruptedException {

		FilterQuery tweetFilterQuery = new FilterQuery();

		String keywords[] = {"enter the keywords you want to look for as strings separated by commas"};
		
		tweetFilterQuery.track(keywords);


		TopologyBuilder topoBuilder = new TopologyBuilder();
		topoBuilder.setSpout("twitterSpout", new TwitterSpout(consumerKey,
				consumerSecret, accessToken, accessTokenSecret,
				tweetFilterQuery));
		topoBuilder.setBolt("mongoDBBolt", new MongoDBBolt()).shuffleGrouping(
				"twitterSpout");

		Config conf = new Config();
		conf.setDebug(false);
		conf.setMaxTaskParallelism(3);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("twitter-collect", conf,
				topoBuilder.createTopology());
		Thread.sleep(600000);
		cluster.shutdown();	
	}

}
