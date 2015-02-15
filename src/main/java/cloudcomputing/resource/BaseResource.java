package cloudcomputing.resource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;

import org.apache.commons.collections.comparators.ComparableComparator;
import org.apache.commons.lang.reflect.ConstructorUtils;
import org.apache.commons.lang.time.StopWatch;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.s3.AmazonS3;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.inject.Injector;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

/**
 * Base Class of All JAX-RS Resources
 * 
 * Use this as a suitable Extension Point :)
 */
public class BaseResource {
	public static final String ID_MASK = "{ id: [^/]+ }";
	
	protected final Logger logger = LoggerFactory.getLogger(this.getClass());
	
	@Inject
	protected ObjectMapper objectMapper;

	@Inject
	protected Injector injector;
	
	@Inject
	AmazonEC2 ec2;

	@Inject
	AmazonS3 s3;

	@Inject
	AmazonDynamoDB dynamoDb;
	
	StopWatch stopWatch = new StopWatch();
	
	String tweetDataTableName = "tweetData";
	
	Authentication hosebirdAuth = new OAuth1("kd6a5RqpYbo2OzqiW7tsmp9Nd", 
			"u9wMUAX6tCaVmJ6tAyy9djBjBNjrgZKDv1EMY9t9iHrPTGRBve", 
			"3010596682-mMFXw3FuVIGHBhCCsHPhRqxE4qtepUptLdgwGnG", 
			"51kjhqdtQNTReY9usQwCghxkqBhlGbcYp8Rv4Txl3F3lD");
	
	BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
	
	BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);
	
	Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
	
	StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
	
	List<Long> followings = Lists.newArrayList(1234L, 566788L);
	
	List<String> terms = Lists.newArrayList("api");
	
	ClientBuilder builder = new ClientBuilder()
	  .name("Hosebird-Client-01")
	  .hosts(hosebirdHosts)
	  .authentication(hosebirdAuth)
	  .endpoint(hosebirdEndpoint)
	  .processor(new StringDelimitedProcessor(msgQueue))
	  .eventMessageQueue(eventQueue);

	Client hosebirdClient = builder.build();
	
	protected <K extends BaseResource> K createResource(Class<K> clazz, Object... args) throws Exception {
		if (null != injector.getBinding(clazz))
			return injector.getInstance(clazz);
		
		@SuppressWarnings("unchecked")
		K result = (K) ConstructorUtils.invokeConstructor(clazz, args);
		
		injector.injectMembers(result);
		
		return result;
	}
	
	@GET
	@Produces("text/plain")
	@Path("/info")
	public String getResourceClass() {
		return getClass().getName();
	}
	
	@GET
	@Produces("text/plain")
	@Path("/start")
	public void start() {
		stopWatch.start();
		ec2.describeInstances();
		s3.listBuckets();		

		hosebirdEndpoint.followings(followings);
		hosebirdEndpoint.trackTerms(terms);
		
		hosebirdClient.connect();
		this.processTweet();
	
		hosebirdClient.stop();
		stopWatch.stop();
		
		System.out.println("Scan arrété !");
	}
	
	private void processTweet()
	{
		System.out.println("Scan démarré !");
		while (!hosebirdClient.isDone()) {
			String msg = null;
			try {
				msg = msgQueue.take();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			this.addTweet(this.treatment(msg));
		}
	}
	
	private void addTweet(Tweet tweet)
	{
		Map<String,AttributeValue> item = new HashMap<String, AttributeValue>();
		item.put("Id", new AttributeValue().withS(tweet.getIdTweet()));
		item.put("Id_User", new AttributeValue().withS(tweet.getIdUser()));
		item.put("Coordinates", new AttributeValue().withS(tweet.getCoordinates()));
		item.put("Date", new AttributeValue().withS(tweet.getDate()));
		item.put("Lang", new AttributeValue().withS(tweet.getLang()));
		item.put("Text", new AttributeValue().withS(tweet.getText()));
		
		dynamoDb.putItem(new PutItemRequest("tweetData", item));
	}
	
	@Path("/number")
    @Produces("text/plain")
    @GET
    public String getNumber(){
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		ec2.describeInstances();
		s3.listBuckets();
		
		ScanRequest scanRequest = new ScanRequest()
    	.withTableName(tweetDataTableName);
		
		ScanResult result = dynamoDb.scan(scanRequest);
		
		stopWatch.stop();
		
		return "Il y a " + result.getCount() + " Tweet(s) dans la base.";
    }
	
    @Path("/lang/{lang}")
    @Produces("text/plain")
    @GET
    public String getTweetByLang(@PathParam("lang") String lang){
        String resultItem = "";
        
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<String, AttributeValue>();
        expressionAttributeValues.put(":lang", new AttributeValue().withS("\"en\""));
        
        ScanRequest scanRequest = new ScanRequest()
        	.withTableName(tweetDataTableName)
        	.withFilterExpression("Lang = :lang")
        	.withProjectionExpression("Id")
        	.withExpressionAttributeValues(expressionAttributeValues);
       
        ScanResult result = dynamoDb.scan(scanRequest);
        for(Map<String,AttributeValue> item : result.getItems()) {
        	resultItem += item.toString() + "\n";
        }
        return "Le nombre de Tweets en " + lang + " est de : " + result.getCount() + "\n" + resultItem;
    }
	
	public Tweet treatment(String msg)
	{
		Tweet tweet = new Tweet();
		msg = msg.substring(1, msg.length()-3);

		String[] msgSplit = msg.split(",");
		
		for(int i = 0; i < msgSplit.length; i++)
		{
			String[] infoSplit = msgSplit[i].split(":");
			String text = "";
			for(int k = 1; k < infoSplit.length; k++)
			{
				text += infoSplit[k] + " ";
			}
			if(infoSplit[0].equals("\"created_at\"") && tweet.getDate().equals("")) {
				tweet.setDate(text);
			} else if (infoSplit[0].equals("\"id\"") && tweet.getIdTweet().equals("")) {
				tweet.setIdTweet(text);
			} else if (infoSplit[0].equals("\"user\"") && tweet.getIdUser().equals("")) {
				tweet.setIdUser(text.substring(6));
			} else if (infoSplit[0].equals("\"lang\"") && tweet.getLang().equals("")) {
				tweet.setLang(text);
			} else if (infoSplit[0].equals("\"coordinates\"") && tweet.getCoordinates().equals("")) {
				tweet.setCoordinates(text);
			} else if (infoSplit[0].equals("\"text\"") && tweet.getText().equals("")) {
				tweet.setText(text);
			}
		}
		return tweet;
	}
}
