package cloudcomputing.resource;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.amazonaws.AmazonServiceException;
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


public class main {	
	public static void main(String[] args) {
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
			Tweet tweet = treatment(msg);
			try {
				System.out.println("Ajout du tweet en base");
				worker.loadSampleProducts(tweet);
		    } catch (AmazonServiceException ase) {
		    	System.err.println("Data load script failed.");
		    }			
			//profit(tweet);
		}
		hosebirdClient.stop();
	}
	
	public static Tweet treatment(String msg)
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
		System.out.println(tweet.toString());
		return tweet;
	}
}
