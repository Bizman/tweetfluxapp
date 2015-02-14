package cloudcomputing.resource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;

import org.apache.commons.lang.reflect.ConstructorUtils;
import org.apache.commons.lang.time.StopWatch;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
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
	
	String tweetDataTableName = "tweetData";
	
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
	public String start() {
		int counter = 0;
		String resultString = "";
		
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		ec2.describeInstances();
		s3.listBuckets();

		
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
		BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);

		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		List<Long> followings = Lists.newArrayList(1234L, 566788L);
		List<String> terms = Lists.newArrayList("api");
		hosebirdEndpoint.followings(followings);
		hosebirdEndpoint.trackTerms(terms);

		Authentication hosebirdAuth = new OAuth1("kd6a5RqpYbo2OzqiW7tsmp9Nd", 
													"u9wMUAX6tCaVmJ6tAyy9djBjBNjrgZKDv1EMY9t9iHrPTGRBve", 
													"3010596682-mMFXw3FuVIGHBhCCsHPhRqxE4qtepUptLdgwGnG", 
													"51kjhqdtQNTReY9usQwCghxkqBhlGbcYp8Rv4Txl3F3lD");
		
		ClientBuilder builder = new ClientBuilder()
		  .name("Hosebird-Client-01")
		  .hosts(hosebirdHosts)
		  .authentication(hosebirdAuth)
		  .endpoint(hosebirdEndpoint)
		  .processor(new StringDelimitedProcessor(msgQueue))
		  .eventMessageQueue(eventQueue);

		Client hosebirdClient = builder.build();		
		hosebirdClient.connect();
		
		while (!hosebirdClient.isDone()) {
			String msg = null;
			try {
				msg = msgQueue.take();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			Tweet tweet = treatment(msg);
			try {
				Map<String,AttributeValue> item = new HashMap<String, AttributeValue>();
				item.put("Id", new AttributeValue().withS(tweet.getIdTweet()));
				item.put("Id_User", new AttributeValue().withS(tweet.getIdUser()));
				item.put("Coordinates", new AttributeValue().withS(tweet.getCoordinates()));
				item.put("Date", new AttributeValue().withS(tweet.getDate()));
				item.put("Lang", new AttributeValue().withS(tweet.getLang()));
					
				PutItemRequest putItemRequest = new PutItemRequest("tweetData", item);
				
				dynamoDb.putItem(putItemRequest);
				break;
		    } catch (AmazonServiceException ase) {
		    	System.err.println("Data load script failed.");
		    }
		}
		hosebirdClient.stop();
			
		ScanRequest scanRequest = new ScanRequest()
	    	.withTableName(tweetDataTableName);
			
		ScanResult result = dynamoDb.scan(scanRequest);
		for (Map<String, AttributeValue> item : result.getItems()){
			resultString += item.toString();
			counter++;
		}	    	
		
		stopWatch.stop();

		return "OK: " + dynamoDb.listTables().toString() + "\n" + "Counter : " + counter + "\n" + resultString;
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
