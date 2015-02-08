import java.text.SimpleDateFormat;
import java.util.Arrays;

import java.util.HashSet;


import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;


public class Worker {
	private DynamoDB dynamoDB;
	private SimpleDateFormat dateFormatter;
	private String tweetDataTableName;
	private AmazonDynamoDBClient dbClient;
	private Table table;
	
	public Worker() {
		System.out.println("Initialisation de la connexion avec la base");
		
		this.dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
		this.tweetDataTableName = "tweetData";
		this.dbClient = new AmazonDynamoDBClient(new ProfileCredentialsProvider());
		this.dbClient.setRegion(Regions.EU_WEST_1);
		this.dynamoDB = new DynamoDB(this.dbClient);
		this.table = dynamoDB.getTable(this.tweetDataTableName);
	}
	
	public void loadSampleProducts(Tweet tweet){
		try{
			System.out.println("Adding data to " + this.tweetDataTableName);
			
			Item item = new Item()
				.withPrimaryKey("Id", tweet.getIdTweet())
				.withString("Id_User", tweet.getIdUser())
				.withString("Text", tweet.getCoordinates())
				.withString("Date", tweet.getDate())
				.withString("Lang", tweet.getLang());
			
			table.putItem(item);
			System.out.println("Item ajouté !");
		} catch (Exception e) {
			System.err.println("Failed to create item in" + this.tweetDataTableName);
			System.err.println(e.getMessage());
		}	
	}
}