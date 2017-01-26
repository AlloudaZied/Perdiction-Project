//package org.jtcb_consulting.twitter.sparkStreaming;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.TableName;
//import org.apache.hadoop.hbase.client.Put;
//import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.streaming.Duration;
//import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
//import org.apache.spark.streaming.api.java.JavaStreamingContext;
//
//import com.cloudera.spark.hbase.JavaHBaseContext;
//
//public class StreamingBulk {
//	 private StreamingBulk() {}
//
//	 
//
//	    
//
//	    SparkConf sparkConf = new SparkConf().setAppName("JavaHBaseStreamingBulkPutExample ")
//	    		.setMaster("local[*]");
//
//	    JavaSparkContext jsc = new JavaSparkContext(sparkConf);
//
//	    try {
//	      JavaStreamingContext jssc =
//	              new JavaStreamingContext(jsc, new Duration(1000));
//
//	      JavaReceiverInputDStream<String> javaDstream =
//	              jssc.socketTextStream("localhost", Integer.parseInt("9999"));
//
//	      Configuration conf = HBaseConfiguration.create();
//
//	      JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);
//
//	      hbaseContext.streamBulkPut(javaDstream,
//	              "",
//	              new PutFunction(),true);
//	    } finally {
//	      jsc.stop();
//	    }
//	  }
//
//	  public static class PutFunction implements Function<String, Put> {
//
//	    private static final long serialVersionUID = 1L;
//
//	    public Put call(String v) throws Exception {
//	      String[] part = v.split(",");
//	      Put put = new Put(Bytes.toBytes(part[0]));
//
//	      put.add(Bytes.toBytes(part[1]),
//	              Bytes.toBytes(part[2]),
//	              Bytes.toBytes(part[3]));
//	      return put;
//	    }
//
//	  }
//	}
//}
