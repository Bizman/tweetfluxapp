package cloudcomputing.resource;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.Lists;
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
 * Root Resource Class. Represents a single entry-point for the whole REST
 * Application, in order to ease on maintenance
 */
@Path("/")
@Produces(MediaType.APPLICATION_JSON)
public class RootResource extends BaseResource {
	@Path("/debug")
	@GET
	public ObjectNode getAllHeaders(@Context HttpHeaders httpHeaders) {
		ObjectNode result = objectMapper.createObjectNode();

		Set<Entry<String, List<String>>> entrySet = httpHeaders
				.getRequestHeaders().entrySet();

		for (Entry<String, List<String>> entry : entrySet) {
			String key = entry.getKey();
			JsonNode value = null;

			if (1 == entry.getValue().size()) {
				value = new TextNode(entry.getValue().get(0));
			} else {
				ArrayNode arrayNode = objectMapper.createArrayNode();

				for (String v : entry.getValue())
					arrayNode.add(v);

				value = arrayNode;
			}

			result.put(key, value);
		}

		return result;
	}
	
	@Path("/hashtag")
	@GET
	public String getHashtag(@Context HttpHeaders httpHeaders) {
	
		return "coucou tout le monde Olivier/Eric !";
	}

	@Path("/remote")
	@GET
	public String getRemoteAddress(@Context HttpServletRequest request) {
		return request.getRemoteHost();
	}
	
	@Path("/start")
	@GET
	public void getStart(@Context HttpServletRequest request) {
		Worker worker = new Worker();
		
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
			Tweet tweet = worker.treatment(msg);
			try {
				System.out.println("Ajout du tweet en base");
				worker.loadSampleProducts(tweet);
		    } catch (AmazonServiceException ase) {
		    	System.err.println("Data load script failed.");
		    }
		}
	}
	
	@Path("/number")
	@GET
	public String getNumberTweet(@Context HttpServletRequest request) {
		int counter = 0;
		
		String tweetDataTableName = "tweetData";
		AmazonDynamoDBClient dbClient = new AmazonDynamoDBClient(new ProfileCredentialsProvider());
		dbClient.setRegion(Regions.EU_WEST_1);
		DynamoDB dynamoDB = new DynamoDB(dbClient);
		Table table = dynamoDB.getTable(tweetDataTableName);

		ScanRequest scanRequest = new ScanRequest()
		    .withTableName(tweetDataTableName);

//		ScanResult result = dbClient.scan(scanRequest);
//		for (Map<String, AttributeValue> item : result.getItems()){
//			counter++;
//		}
		
    	return table.getTableName()+ " "+counter;
	}
	
	@Path("/health")
	public HealthResource getHealthResource() throws Exception {
		return super.createResource(HealthResource.class);
	}
	
	@Path("/carto/{coordinate}/{hashtag}")
	@GET
	public String getCartographie(){return null;}
	
	@Path("/ddd")
	@DELETE
	public void deletCartographie(){}
	


}
