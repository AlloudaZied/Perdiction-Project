//package org.jtcb_consulting.twitter.sparkStreaming;
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Map;
//
//import org.apache.log4j.Level;
//import org.apache.log4j.Logger;
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.function.FlatMapFunction;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.api.java.function.Function2;
//import org.apache.spark.api.java.function.PairFunction;
//import org.apache.spark.streaming.Durations;
//import org.apache.spark.streaming.api.java.JavaDStream;
//import org.apache.spark.streaming.api.java.JavaPairDStream;
//import org.apache.spark.streaming.api.java.JavaStreamingContext;
//import org.apache.spark.streaming.kafka.KafkaUtils;
//
//import scala.Tuple2;
//
///**
// * Hello world!
// *
// */
//public class NGram 
//{    
//	static List list = new ArrayList<String>();
//    public static void main( String[] args ) throws InterruptedException
//    {
////       NGram ng = new NGram();
////       SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("testspark");
////       JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1/2));
////  	   Map<String, Integer> topics = new HashMap<String, Integer>();
////  	   topics.put("tweetstest", 1);
////  	   JavaPairDStream<String, String> messages =KafkaUtils.createStream(jssc, "latitude:2181", "gp32", topics);
////       JavaDStream<String> rdd =  messages.map(s->s._2);
////       JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
////    	      @Override
////    	      public String call(Tuple2<String, String> tuple2) {
////    	        return tuple2._2();
////    	      }
////    	});
////       JavaDStream<String> ngram = lines.flatMap(new FlatMapFunction<String, String>() {
////    	      @Override
////    	      public Iterator<String> call(String x) throws IOException {   	    	  
////    	        return generateNgrams(3,x).iterator();
////    	      }
////    	});
////       JavaPairDStream<String, Integer> ngramCounts = ngram.mapToPair(
////    		      new PairFunction<String, String, Integer>() {
////    		        @Override
////    		        public Tuple2<String, Integer> call(String s) {
////    		          return new Tuple2<>(s, 1);
////    		        }
////    		      }).reduceByKey(
////    		        new Function2<Integer, Integer, Integer>() {
////    		        @Override
////    		        public Integer call(Integer i1, Integer i2) {
////    		          return i1 + i2;
////    		        }
////    		});
////       ngramCounts.print();
////  	   jssc.start();
////  	   jssc.awaitTermination();
//    	test();
//    	
//    }
//    /**
//     * get N-gram
//     *  
//     *  
//     *  **/
//    public  static List generateNgrams(int N, String sent) throws IOException {
//  	  String[] tokens = sent.split(" "); 
//  	  List list = new ArrayList<String>();
//  	  for(int k=0; k<(tokens.length-N+1); k++){
//  	    String s="";
//  	    int start=k;
//  	    int end=k+N;
//  	    for(int j=start; j<end; j++){
//  	     s=s+" "+tokens[j];
//  	                  }    
//  	      s=processTweets(s);
//  	      list.add(s);
//  	     s = s+"\n";
//  	  }
//	return list;
//  	  } 
//    /**
//     * clean String
//     *  
//     *  
//     *  **/
//  	public static String processTweets(String tweets)
//    {
//    	String []splitted = tweets.split(" ");
//    	String tweetsprocessed = "";
//    	for (String word : splitted)
//    	{
//       	tweetsprocessed =tweetsprocessed +" "+word.replaceAll("#", "");	
//         }	
//    	return tweetsprocessed ;
//    }
//  	public static void test() throws InterruptedException
//  	{
//  		 Logger.getLogger("org").setLevel(Level.OFF);
//         Logger.getLogger("akka").setLevel(Level.OFF);
//  		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("testspark");
//      JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1/2));
// 	   Map<String, Integer> topics = new HashMap<String, Integer>();
// 	   topics.put("testtt1", 3);
// 	   JavaPairDStream<String, String> messages =KafkaUtils.createStream(jssc, "latitude:2181", "gp11", topics);
//      JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
//   	      @Override
//   	      public String call(Tuple2<String, String> tuple2) {
//   	        return tuple2._2();
//   	      }
//   	});
//      JavaDStream<String> ngram = lines.flatMap(new FlatMapFunction<String, String>() {
//   	      @Override
//   	      public Iterator<String> call(String x) throws IOException {   	    	  
//   	        return Arrays.asList(x.split(" ")).iterator();
//   	      }
//   	});
//      JavaPairDStream<String, Integer> ngramCounts = ngram.mapToPair(
//   		      new PairFunction<String, String, Integer>() {
//   		        @Override
//   		        public Tuple2<String, Integer> call(String s) {
//   		          return new Tuple2<>(s, 1);
//   		        }
//   		      }).reduceByKey(
//   		        new Function2<Integer, Integer, Integer>() {
//   		        @Override
//   		        public Integer call(Integer i1, Integer i2) {
//   		          return i1 + i2;
//   		        }
//   		});
//      ngramCounts.print();
// 	   jssc.start();
// 	   jssc.awaitTermination();	
//  		
//  		
//  	}
//}
