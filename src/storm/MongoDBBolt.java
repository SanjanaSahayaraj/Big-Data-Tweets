package storm;
import com.mongodb.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Timer;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.Calendar;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import twitter4j.Status;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.microsoft.windowsazure.services.servicebus.ServiceBusConfiguration;
import com.microsoft.windowsazure.services.servicebus.ServiceBusContract;
import com.microsoft.windowsazure.services.servicebus.ServiceBusService;


public class MongoDBBolt extends BaseRichBolt {
public String mesg="";
long utflength=0;
BigInteger bi1=BigInteger.valueOf(0);


	private OutputCollector _outputCollector;
	com.microsoft.windowsazure.Configuration config;
	ServiceBusContract service;

	@Override
	public void execute(Tuple tuple) {

		String uid = tuple.getStringByField("keyword");
		Status twitte = (Status) tuple.getValueByField("status");
		String jsonStatus = null;

		try {
			jsonStatus = getJSONString(twitte);

			Properties props = new Properties();
	        props.put("metadata.broker.list", "kafkaser:9092");
	       props.put("serializer.class", "kafka.serializer.StringEncoder");
	       props.put("message.max.bytes", "1000000");
	       props.put("fetch.message.max.bytes", "10000000");	
	        props.put("request.required.acks", "1");
	        ProducerConfig config = new ProducerConfig(props);
	        
	        Producer<String, String> producer = new Producer<String, String>(config);

	               String ip="kafkaser";//hostname
	               String msg=jsonStatus;
	               final byte[] utf8Bytes = msg.getBytes("UTF-8");
	               utflength+=utf8Bytes.length;
	               bi1=bi1.add(BigInteger.valueOf(utflength));
	  
	               System.out.println("utf 8 bytes from producer : "+bi1);

	               KeyedMessage<String, String> data = new KeyedMessage<String, String>("twittertopic", ip, msg);
	       
	               producer.send(data);
	   
	       producer.close();
	
			
		}
		catch (JSONException e1) {
			e1.printStackTrace();
		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
		}
		_outputCollector.emit(new Values(uid, jsonStatus));

		_outputCollector.ack(tuple);
	}

	private String getJSONString(Status status) throws JSONException {
		JSONObject innerObject = new JSONObject();

		innerObject.put("status", status);
		return innerObject.toString();
	}

	@Override
	public void prepare(Map arg0, TopologyContext topoContext,
			OutputCollector outputCollector) {

		try{
		MongoClient mongoClient = new MongoClient("ip_address", 27017);
		 _outputCollector = outputCollector;
		config = ServiceBusConfiguration.configureWithSASAuthentication("provide the 4 authentication credentials here");
		service = ServiceBusService.create(config);
	}
		catch(Exception e)
		{
			System.out.println("exception caught in prepare");
		}
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("keyword", "status"));
	}

	@Override
	public void cleanup() {
		super.cleanup();
	}


	
}