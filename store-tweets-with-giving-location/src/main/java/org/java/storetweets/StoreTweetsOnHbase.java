package org.java.storetweets;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import twitter4j.FilterQuery;
import twitter4j.JSONObject;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

/**
 * The StoreTweetsONHbase program implements an application that simply displays
 * store TWEETS on HBASE with the unique key id of user.
 * 
 * @author SENDI ZIED
 * @version 1.0
 * @since 2016-11-30
 */
public class StoreTweetsOnHbase {
	public static Integer key = null ;
	/**
	 * the main method that store streamed flow from twitter to HBASE. this
	 * method do the following steps : -Create a configuration using
	 * org.apach.hadoop.Configuration,and then set the ZNODE PARENT configuration for
	 * HBASE. -create connection using org.apach.hadoop.hbase.client.Connection
	 * -Create TwitterStream object which is used to get TWEETS from twitter
	 * (using Twitter4j API) -As result of response from twitter it will be
	 * saved on HBASE this method is not designed for processing data from
	 * twitter ,it simply store it on HBASE as it received We note also that the
	 * administrative API was not used in this method so you should create your
	 * own table from command shell before running the APP .
	 * 
	 * @param NameTable
	 *            of table Name of table created with shell command .
	 * @param TWITTER_CONSUMER_KEY
	 *            see https://apps.twitter.com/app .
	 * @param TWITTER_SECRET_KEY
	 *            see https://apps.twitter.com/app .
	 * @param TWITTER_ACCESS_TOKEN
	 *            see https://apps.twitter.com/app .
	 * @param TWITTER_ACCESS_TOKEN_SECRET
	 *            see https://apps.twitter.com/app
	 * @param Configuration : /HBASE-UNSEQURE 
	 * @param  startKey of the storage in table       
	 */
	
	public static void main(String[] args) throws IOException {	
		  key = new Integer(args[6]);
		if (args.length<5)
		{
			System.out.println("You should give token and key : for more details see https://apps.twitter.com/app");
			System.out.println("And then run this commande : TableName ConsumeKey ConsumerSecret  AccessToken AccessTokenSecret Configuration");
			System.exit(-1);	
		}
		Configuration configuration = new Configuration();
		if (args[5] !=null){
			configuration.set("zookeeper.znode.parent", args[5]);
		}	
		Connection connection = ConnectionFactory.createConnection(configuration);
		Table table = connection.getTable(TableName.valueOf(args[0]));
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true).setOAuthConsumerKey(args[1]).setOAuthConsumerSecret(args[2])
				.setOAuthAccessToken(args[3]).setOAuthAccessTokenSecret(args[4]);
		TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
		StatusListener listener = new StatusListener() {
			
			public void onStatus(Status status) {
				JSONObject json = new JSONObject(status);
//				System.out.println(JSON);
				try {
      				Put put = new Put(key.toString().getBytes());
					put.addColumn("tweets".getBytes(), "value".getBytes(), json.toString().getBytes());
					table.put(put);
					key ++;
					
					
				} catch ( IOException e) {
					System.out.println(e.getMessage());
				}
			}

			public void onException(Exception ex) {
				ex.printStackTrace();
			}

			public void onDeletionNotice(StatusDeletionNotice arg0) {
			}

			public void onScrubGeo(long arg0, long arg1) {
			}

			public void onStallWarning(StallWarning arg0) {
			}

			public void onTrackLimitationNotice(int arg0) {
			}
		};

		twitterStream.addListener(listener);
		System.out.println("Saving ...");
		FilterQuery filterQuery = new FilterQuery();
		double[][] locations = { { -79, 40 }, { -70, 41 } }; // those are the
																// boundary from
																// New York City
		filterQuery.locations(locations);
		String[] follow = {"MSFT", "MICROSOFT","APPLE","EXXONMOBIL","JOHNSON & JOHNSON","JP MORGAN CHASE","GENERAL ELECTRIC","PROCTER & GAMBLE"				
		,"WAL-MART STORES","CHEVRON","VERIZON COMMUNICATIONS","PFIZER","COCA-COLA","MERCK & CO","INTEL CORPORATION","WALT DISNEY","HOME DEPOT",
        "IBM - INTERNATIONAL BUSINESS MACHINES","UNITEDHEALTH GROUP","CISCO SYSTEMS","VISA INC","3M - MINNESOTA MINING & MANUFACTURING","MCDONALD'S",
        "BOEING","UNITED TECHNOLOGIES","GOLDMAN SACHS","NIKE","AMERICAN EXPRESS","DUPONT","CATERPILLAR","TRAVELERS COMPANIES"
		};
		twitterStream.filter(filterQuery.track(follow));
	}
}