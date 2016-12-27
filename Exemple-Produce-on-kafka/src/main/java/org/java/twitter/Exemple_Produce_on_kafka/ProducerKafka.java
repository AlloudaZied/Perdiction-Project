package org.java.twitter.Exemple_Produce_on_kafka;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;


/**
 * This APP stream data from HBASE and put them on a topic
 *   
 * @param args[0] 
 *        This is the name of the HTABLE on HBASE 
 * @param args[1]  
 *        This is the name of the topic on the broker
 * @param args[2]  
 *        This is the configuration of ZNODE
 * @param args[3] 
 *        This is the property CONFIG of the KafkaProducer (@ of the broker KAFKA LISTNER)
 *
 */

public class ProducerKafka extends ProducerInt
  {
	final static Logger logger = Logger.getLogger(ProducerKafka.class);

   public static void main( String[] args ) throws IOException    
        {   
	  final String  topicname = args[3];
	  final String  brokerlist = args[2];
	  final byte [] StartKey = args[0].getBytes();
	  final byte [] EndKey  =  args[1].getBytes();
      Configuration configuration = new Configuration();
      ProducerInt prod = new ProducerInt();      
      prod.configureProducer(brokerlist);
      configuration.set("zookeeper.znode.parent", "/hbase-unsecure");
//      configuration.set("hbase.zookeeper.quorum","192.168.1.161,192.168.1.162,192.168.1.163");
//      configuration.set("hbase.zookeeper.property.clientPort", "2181");
      Connection connection = ConnectionFactory.createConnection(configuration);
      Table table = connection.getTable(TableName.valueOf("tweetstableSplitted"));            
      Scan scanner = new Scan(StartKey,EndKey);
      scanner.setMaxVersions(1);
      scanner.addFamily("tweets".getBytes());
      ResultScanner resultScanner = table.getScanner(scanner); 
      System.out.println("Streaming ..." + args[0]);
    try {  
      for (Result result= resultScanner.next();result !=null;result = resultScanner.next())
      {
    	    Get get = new Get(result.getRow());
    	    Result entireRow = table.get(get); 
    	    byte [] value = entireRow.value();
    	    byte [] key = entireRow.getRow();
    	    prod.sendData(topicname, Bytes.toString(key), Bytes.toString(value));      	    
      }      
    }
    catch (Exception e)
    {
    	logger.error(e.getMessage());
    }
    prod.closeProducteur();
    }	
}
