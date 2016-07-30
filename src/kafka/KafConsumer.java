package kafka;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

class KafConsumer extends  Thread {
	long length=0;
	long utflength=0;
	BigInteger bi1=BigInteger.valueOf(0);
	final byte[] utf8Bytes=null;
	 public static void main(String[] argv) throws UnsupportedEncodingException {
	        KafConsumer KafConsumer = new KafConsumer();
	        KafConsumer.start();
	    }
	
	static String timeinsert="";
	public String mesg="";
	public int recordnum=0;
 
    final static String TOPIC = "hostname/ipaddress";
    ConsumerConnector consumerConnector;
 SimpleDateFormat format=new SimpleDateFormat("HH:mm:ss");
 static Date starttime=null;
 
    public KafConsumer(){
        Properties properties = new Properties();
        properties.put("zookeeper.connect","hostname:portnumber");
       properties.put("group.id","name_of_group");
        ConsumerConfig consumerConfig = new ConsumerConfig(properties);
        consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
    }

    @Override
    public void run() {
    	try{
  
    		long before=0,after=0,duration=0;	
    	MongoClient mongoClient = new MongoClient("ipaddress", 27017);
    
		DB dtbs = mongoClient.getDB("insert_database_name");
	
		DBCollection collec = dtbs.getCollection("collection_name");
	
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(TOPIC, new Integer(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
     
        KafkaStream<byte[], byte[]> stream =  consumerMap.get(TOPIC).get(0);
      
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
   
        while(it.hasNext())
        {mesg=new String(it.next().message());
     
            BasicDBObject record = new BasicDBObject("tweet", mesg);
     before=System.currentTimeMillis();
            collec.insert(record);
            length+=mesg.length();
            final byte[] utf8Bytes = mesg.getBytes("UTF-8");
            utflength+=utf8Bytes.length;
            bi1=bi1.add(BigInteger.valueOf(utflength));
            System.out.println("utf 8 bytes till now : "+bi1);

     after=System.currentTimeMillis();
     duration=after-before;
     String timeinsert2=" in milliseconds: "+duration+"  ";
        
        recordnum++;
        Calendar cal=new GregorianCalendar();
        long milsec=0;
		long enHr=cal.get(Calendar.HOUR_OF_DAY);
		milsec=(enHr*60);
		long enMn=cal.get(Calendar.MINUTE);
		long enSc=cal.get(Calendar.SECOND);
		DB timedb=mongoClient.getDB("database_name_same_or_different");
		DBCollection col=timedb.getCollection("another_collection");
		String timeinsert= "Record num "+recordnum+" inserted at "+enHr+" "+enMn+" "+enSc;
	
		BasicDBObject timerec=new BasicDBObject("details of inserted record:  ",timeinsert);

		timerec.append("timetaken", timeinsert2);
		col.insert(timerec);
        }
  
    	}
    	catch(Exception e){e.printStackTrace();}
    }
    private void printMessages(ByteBufferMessageSet messageSet) throws UnsupportedEncodingException {
    	try
    	{

        for(MessageAndOffset messageAndOffset: messageSet) {
            ByteBuffer payload = messageAndOffset.message().payload();
            byte[] bytes = new byte[payload.limit()];
            payload.get(bytes);
            mesg=new String(bytes, "UTF-8");

        }
    }
    	catch(Exception e){System.out.println("error at last function of kaf consumer");}
    }

}



