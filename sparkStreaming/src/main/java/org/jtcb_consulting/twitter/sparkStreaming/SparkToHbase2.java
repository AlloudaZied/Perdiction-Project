//package org.jtcb_consulting.twitter.sparkStreaming;
//
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.client.Put;
//import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.log4j.Level;
//import org.apache.log4j.Logger;
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.FlatMapFunction;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.api.java.function.Function2;
//import org.apache.spark.api.java.function.PairFunction;
//import org.apache.spark.api.java.function.VoidFunction;
//import org.apache.spark.streaming.api.java.JavaDStream;
//import org.apache.spark.streaming.api.java.JavaPairDStream;
//import org.apache.spark.streaming.api.java.JavaPairInputDStream;
//import org.apache.spark.streaming.api.java.JavaStreamingContext;
//import org.apache.spark.streaming.kafka.KafkaUtils;
//
//import com.cloudera.spark.hbase.JavaHBaseContext;
//
//import kafka.serializer.StringDecoder;
//import scala.Tuple2;
//import twitter4j.JSONObject;
///**
// * Hello world!
// *
// */
//public class SparkToHbase2 
//{
//    public static void main( String[] args )
//    {
//      Logger.getLogger("org").setLevel(Level.OFF);
//      Logger.getLogger("akka").setLevel(Level.OFF);
//      Configuration configHbase = HBaseConfiguration.create();
//      configHbase.set("zookeeper.znode.parent", "/hbase-unsecure");
//      SparkConf conf = new SparkConf().setAppName("SparkToHbase").setMaster("local[*]");
//      JavaSparkContext context = new JavaSparkContext(conf);
//      Map<String,Integer> topicMap = new HashMap<String,Integer>();
//   String[] topic = "tweetstest2".split(",");
//     for(String t: topic)
//      {
//         topicMap.put(t, new Integer(1));
//      }
//   Set<String> topicsSet = new HashSet<>(Arrays.asList("tweetstest2".split(",")));
//   Map<String, String> kafkaParams = new HashMap<>();
//   kafkaParams.put("metadata.broker.list", "latitude:6667");
//   kafkaParams.put("auto.offset.reset",    "smallest");
//   kafkaParams.put("group.id", "group-3");
//   JavaStreamingContext jssc = new JavaStreamingContext(context, new org.apache.spark.streaming.Duration(1));
//   JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
//         jssc,
//         String.class,
//         String.class,
//         StringDecoder.class,
//         StringDecoder.class,
//         kafkaParams,
//         topicsSet
//     );
//   JavaDStream<String> values = messages.map(new Function<Tuple2<String,String>, String>() {
//	private static final long serialVersionUID = 1L;
//
//	@Override
//	public String call(Tuple2<String, String> v1) throws Exception {
//		return v1._2;
//	}
//    });
//   JavaDStream<JSONObject> valueJson = values.map(new Function<String, JSONObject>() {
//	/**
//	 * 
//	 */
//	private static final long serialVersionUID = 1L;
//
//	@Override
//	public JSONObject call(String v1) throws Exception {
//			return new JSONObject(v1);
//	}
//    });
//   JavaDStream<String> text = valueJson.map(new Function<JSONObject, String>() {
//		private static final long serialVersionUID = 1L;
//		@Override
//		public String call(JSONObject v1) throws Exception {
//			return v1.getString("text");				
//		}		     
//    });
//   JavaDStream<String> ngram = text.flatMap(new FlatMapFunction<String, String>() {
//		private static final long serialVersionUID = 1L;
//		@Override
//	    public Iterable<String> call(String x) throws IOException {   	    	  
//	        return (Iterable<String>) generateNgrams(3, x).iterator();
//	      }
//	});
//   JavaPairDStream<String, Integer> ngramCounts = ngram.mapToPair(
//		      new PairFunction<String, String, Integer>() {
//				private static final long serialVersionUID = 1L;
//				@Override
//		        public Tuple2<String, Integer> call(String s) {
//		          return new Tuple2<String,Integer>(s, 1);
//		        }
//		      }).reduceByKey(new Function2<Integer, Integer, Integer>() {					
//		private static final long serialVersionUID = 1L;
//		@Override
//		public Integer call(Integer v1, Integer v2) throws Exception {						
//			return v1 + v2;
//		}
//	}); 
//   JavaHBaseContext hbaseContext = new JavaHBaseContext(context,configHbase );
//   try {
//	   ngramCounts.foreachRDD(new VoidFunction<JavaPairRDD<String,Integer>>() {
//		private static final long serialVersionUID = 1L;
//		@Override
//		public void call(JavaPairRDD<String, Integer> t) throws Exception {
//			JavaRDD<String> rddString = t.map(new Function<Tuple2<String,Integer>, String>() {
//				private static final long serialVersionUID = 1L;
//				@Override
//				public String call(Tuple2<String, Integer> v1) throws Exception {
//                      v1._2.toString();
//					return v1._1 + "," + v1._2.toString ();
//				}
//			});	
//			hbaseContext.bulkPut(rddString, "finaltesthbase", new PutFunction(), true);
//		}
//	});
//   }
//   finally {
//   context.stop();
//	}
//    }
//    /**
//   * this method generate an NGRAM from giving text
//   * @param N : number of gram
//   * @param sent : the sentences to be processed
//   *
//   ***/
//  public  static List<String> generateNgrams(int N, String sent) throws IOException {
//	         String[] tokens = sent.split(" "); 
//	         List<String> list = new ArrayList<String>();
//	  for(int k=0; k<(tokens.length-N+1); k++){
//	         String s="";
//	         int start=k;
//	         int end=k+N;
//	    for(int j=start; j<end; j++){
//	           s=s+" "+tokens[j];
//	                  }    
//	           s=processTweets(s);
//	           list.add(s);
//	           s = s+"\n";
//	  }
//	return list;
//	  } 
//  /**
//   * Cleaning NGRAM
//   * @param tweets  : the message to be process
//   ***/
//	public static String processTweets(String tweets)
//  {
//  	String []splitted = tweets.split(" ");
//  	String tweetsprocessed = "";
//  	for (String word : splitted)
//  	  {
//     	tweetsprocessed =tweetsprocessed +" "+word.replaceAll("#", "HASHTAG");	
//       }	
//  	return tweetsprocessed ;
//  }
//	 public static class PutFunction implements Function<String, Put> {
//		    private static final long serialVersionUID = 1L;
//		    public Put call(String v) throws Exception {
//		      String[] cells = v.split(",");
//		      Integer x = new Integer((int)Math.random());
//		      Put put = new Put(Bytes.toBytes(x.toString()));
//		      put.add("descripteur".getBytes(), "ngram".getBytes(),
//		              Bytes.toBytes(cells[1]));
//		      put.add("descripteur".getBytes(), "frequency".getBytes(),
//		              Bytes.toBytes(cells[2]));		      
//		      return put;
//		    }
//		  }
//}
