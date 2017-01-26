package org.jtcb_consulting.twitter.sparkStreaming;


import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import com.cloudera.spark.hbase.JavaHBaseContext;

/**
 * This is a simple example of putting records in HBase
 * with the bulkPut function.
 */
final public class SparkToHbase {
  private SparkToHbase() {}
  public static void main(String[] args) {
    String tableName = "finaltesthbase";
    String columnFamily = "descripteur";
    SparkConf sparkConf = new SparkConf().setAppName( "SparkToHbase" + tableName).setMaster("local[2]");
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);
    try {
      List<String> list = new ArrayList<String>();
      list.add("101," + columnFamily + ",a,101");
      list.add("121," + columnFamily + ",a,121");
      list.add("131," + columnFamily + ",a,131");
      list.add("141," + columnFamily + ",a,141");
      list.add("151," + columnFamily + ",a,151");
      JavaRDD<String> rdd = jsc.parallelize(list);
      Configuration conf = HBaseConfiguration.create();
      conf.set("zookeeper.znode.parent", "/hbase-unsecure");
      JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc,conf );
      hbaseContext.bulkPut(rdd, tableName, new PutFunction(), true);
    } finally {
      jsc.stop();
    }
  }
  public static class PutFunction implements Function<String, Put> {
    private static final long serialVersionUID = 1L;
    public Put call(String v) throws Exception {
      String[] cells = v.split(",");
      System.out.println(v);
      Put put = new Put(Bytes.toBytes(cells[0]));
      put.add(Bytes.toBytes(cells[1]), Bytes.toBytes(cells[2]),
              Bytes.toBytes(cells[3]));
      return put;
    }
  }
}