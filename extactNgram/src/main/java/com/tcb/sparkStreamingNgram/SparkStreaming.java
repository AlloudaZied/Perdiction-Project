package com.tcb.sparkStreamingNgram;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.cloudera.spark.hbase.JavaHBaseContext;

import kafka.serializer.StringDecoder;
import scala.Tuple2;
import twitter4j.JSONObject;

public class SparkStreaming {
	/**
	  * The main function
	  **/
	    public static void main(String args[]) throws InterruptedException, IOException       	     
	       {     
	         Configuration configuration = new Configuration();
	         configuration.set("zookeeper.znode.parent", "/hbase-unsecure");
	         SparkConf sparkConf = new SparkConf().setAppName( "SparkToHbase" + "tableName").setMaster("local[2]");
	         JavaSparkContext jsc = new JavaSparkContext(sparkConf);
	         JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc,configuration );
	         Logger.getLogger("org").setLevel(Level.OFF);
	         Logger.getLogger("akka").setLevel(Level.OFF);
	         Map<String,Integer> topicMap = new HashMap<String,Integer>();
	         String[] topic = "tweetstest2".split(",");
	      for(String t: topic)
	             {
	           topicMap.put(t, new Integer(1));
	             }
	         Set<String> topicsSet = new HashSet<String>(Arrays.asList("tweetstest2".split(",")));
	         Map<String, String> kafkaParams = new HashMap<String,String>();
	         kafkaParams.put("metadata.broker.list", "latitude:6667");
	         kafkaParams.put("auto.offset.reset",    "smallest");
	         kafkaParams.put("group.id", "group-3");
	         JavaStreamingContext jssc = new JavaStreamingContext(jsc, new Duration(10));   
	         JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
	            jssc,
	            String.class,
	            String.class,
	            StringDecoder.class,
	            StringDecoder.class,
	            kafkaParams,
	            topicsSet
	        );
	      JavaDStream<String> value = messages.map(tuple ->tuple._2);  
	      JavaDStream<JSONObject> json = value.map(arg0-> new JSONObject(arg0));			
	      JavaDStream<String> textField = json.map(arg0 ->arg0.getString("text"));
	      JavaDStream<String> ngram = textField.flatMap(line -> generateNgrams(2, line));
	      JavaPairDStream<String, Integer> ngramCounts = ngram.
	    		    mapToPair(arg0 ->new Tuple2<String,Integer>(arg0, 1))
			       .reduceByKey((x,y) -> x+y);
	      JavaDStream<String> HbaseRow = ngramCounts.map(arg0 -> arg0._1 + "|" +arg0._2);
	     
	     hbaseContext.streamBulkPut(HbaseRow, "tableStreaming2", new PutFunction(), true);
		   value.print();
	       jssc.start();
	       jssc.awaitTermination();
	        }
	        /**
	         * this method generate an NGRAM from giving text
	         * @param N : number of gram
	         * @param sent : the sentences to be processed
	         *
	         ***/
	   public  static List<String> generateNgrams(int N, String sent) throws IOException {
		         String[] tokens = sent.split(" "); 
		         List<String> list = new ArrayList<String>();
		       for(int k=0; k<(tokens.length-N+1); k++){
		         String s="";
		         int start=k;
		         int end=k+N;
		       for(int j=start; j<end; j++){
		           s=s+" "+tokens[j];
		                  }    
		           s=processTweets(s);
		           list.add(s);
		           s = s+"\n";
		  }  
	             return list ;
		  } 
	            /**
	             * Cleaning NGRAM
	             * @param tweets  : the message to be process
	             ***/
		public static String processTweets(String tweets)
	         {
		String []splitted = tweets.split(" ");
		String tweetsprocessed = "";
		for (String word : splitted)
		     {
	   	tweetsprocessed =tweetsprocessed +" "+word.replaceAll("#", "HASHTAG");	
	         }	
		return tweetsprocessed ;
	      }
		public static class PutFunction implements Function<String, Put> {
		    private static final long serialVersionUID = 1L;
		    public Put call(String v) throws Exception {
		    	 String[] part = v.split(",");
		    	 System.out.println(v);
		    	  Put put = new Put(Bytes.toBytes(part[0]));
		         put.add(Bytes.toBytes("descripteur"),
		                 Bytes.toBytes("deuxgram"),
		                 Bytes.toBytes(part[0]));
		         put.add(Bytes.toBytes("descripteur"),
		                 Bytes.toBytes("frequency"),
		                 Bytes.toBytes(part[1]));		    	 
		      return put;
		    }
		  }
	}