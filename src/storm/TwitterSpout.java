package storm;

import backtype.storm.Config;
import backtype.storm.topology.OutputFieldsDeclarer;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

import javax.management.RuntimeErrorException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterObjectFactory;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;

import com.google.gson.JsonElement;
import com.google.gson.JsonIOException;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

public class TwitterSpout extends BaseRichSpout {
	public static Logger LOG = LoggerFactory.getLogger(TwitterSpout.class);
	boolean _isDistributed;
	SpoutOutputCollector _collector;
	Integer uid = 0;

	Long count = 0l;

	private final String _accessTokenSecret;
	private final String _accessToken;
	private final String _consumerSecret;
	private final String _consumerKey;
	private FilterQuery _tweetFilterQuery;
	ConfigurationBuilder _configurationBuilder;
	private twitter4j.TwitterStream _twitterStream;
	private LinkedBlockingQueue<Status> statusQueue;

	public TwitterSpout(String consumerKey, String consumerSecret,
			String accessToken, String accessTokenSecret,
			FilterQuery filterQuery) {
		if (consumerKey == null || consumerSecret == null
				|| accessToken == null || accessTokenSecret == null) {
			throw new RuntimeException("Twitter4j authentication field is null");
		}
		_consumerKey = consumerKey;
		_consumerSecret = consumerSecret;
		_accessToken = accessToken;
		_accessTokenSecret = accessTokenSecret;
		_tweetFilterQuery = filterQuery;
	}

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		statusQueue = new LinkedBlockingQueue<Status>();
		_collector = collector;
		_configurationBuilder = new ConfigurationBuilder();
		_configurationBuilder.setOAuthConsumerKey(_consumerKey)
				.setOAuthConsumerSecret(_consumerSecret)
				.setOAuthAccessToken(_accessToken)
				.setOAuthAccessTokenSecret(_accessTokenSecret)
				.setJSONStoreEnabled(true);

		_twitterStream = new TwitterStreamFactory(_configurationBuilder.build())
				.getInstance();
		StatusListener listener = new StatusListener() {

			@Override
			public void onException(Exception arg0) {
				System.out.println("Error occured: " + arg0.getMessage());
				arg0.printStackTrace();
			}

			@Override
			public void onTrackLimitationNotice(int arg0) {
				System.out.println("Track limitation notice for " + arg0);
			}

			@Override
			public void onStatus(Status status) {
				if (meetsConditions(status))
					statusQueue.offer(status);
			}

			@Override
			public void onStallWarning(StallWarning arg0) {
				// TODO Auto-generated method stub

			}

			@Override
			public void onScrubGeo(long arg0, long arg1) {
				// TODO Auto-generated method stub

			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice arg0) {
				// TODO Auto-generated method stub

			}
		};
		_twitterStream.addListener(listener);

		if (_tweetFilterQuery == null) {
			_twitterStream.sample();
		} else {
			_twitterStream.filter(_tweetFilterQuery);
		}
	}

	private boolean meetsConditions(Status status) {
		return true;
	}

	public void close() {
		_twitterStream.shutdown();
		super.close();
	}

	public void nextTuple() {
		Status twitte = statusQueue.poll();
		if (twitte == null)
			Utils.sleep(1000);
		else {
			_collector.emit(new Values(uid.toString(), twitte));
			uid++;
		}
	}

	public void ack(Object msgId) {

	}

	public void fail(Object msgId) {

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("keyword", "status"));
	}

	public ArrayList<String> getListOfKeywords(Status status,
			String[] listOfKeyWords) {

		ArrayList<String> listOfKeyworsMatched = new ArrayList<String>();
		for (String keyword : listOfKeyWords) {
			if (isMatched(keyword, status.getText()))
				listOfKeyworsMatched.add(keyword);
		}
		return null;
	}

	public boolean isMatched(String keyword, String tweetText) {

		return true;
	}
}
