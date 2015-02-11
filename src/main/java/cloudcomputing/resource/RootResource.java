package cloudcomputing.resource;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

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

	@Path("/remote")
	@GET
	public String getRemoteAddress(@Context HttpServletRequest request) {
		return request.getRemoteHost();
	}
	
	@Path("/health")
	public HealthResource getHealthResource() throws Exception {
		return super.createResource(HealthResource.class);
	}
	
	@Path("/hashtag/")
	@GET
	public String getHashtag(@Context HttpHeaders httpHeaders) {
		
		return "coucou olivier";
	}
	
	@Path("/number/")
	@GET
	public String getNumberTweet(@Context HttpHeaders httpHeaders) {
		int couter = 0;
		
		String tweetDataTableName = "tweetData";
		AmazonDynamoDBClient dbClient = new AmazonDynamoDBClient(new ProfileCredentialsProvider());
		dbClient.setRegion(Regions.EU_WEST_1);
		DynamoDB dynamoDB = new DynamoDB(dbClient);
		Table table = dynamoDB.getTable(tweetDataTableName);
    	   
        Map<String, Object> expressionAttributeValues = new HashMap<String, Object>();
        expressionAttributeValues.put(":Id", 1);
		
    	ItemCollection<ScanOutcome> items = table.scan(
    		"Id = :id",
    	    null, expressionAttributeValues);
        
        System.out.println("Scan of " + tweetDataTableName + " for items");
      	Iterator<Item> iterator = items.iterator();
		while (iterator.hasNext()) {
    		couter++;
    	}
    	
    	return "Nombre de tweet dans la base : "+couter;
	}
	
	@Path("/carto/{coordinate}/{hashtag}")
	@GET
	public String getCartographie(){return null;}
	
	@Path("/ddd")
	@DELETE
	public void deletCartographie(){}
	


}
