package org.java.twitter.Copy_table_hbase_with_new_key;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;


/**
 *
 *
 */
public class Copy 
{
    public static void main( String[] args ) throws IOException
    {   
    	Integer  keys = new Integer(args[0]);
    	Configuration conf = new Configuration();
//        conf.set("hbase.zookeeper.quorum","192.168.1.161,192.168.1.162,192.168.1.163");
//        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");
        Connection conn = ConnectionFactory.createConnection(conf);
        Connection conne= ConnectionFactory.createConnection(conf);
		Table tablesource = conn.getTable(TableName.valueOf("tweetstable"));
        Table tablereceiver = conne.getTable(TableName.valueOf("montesttable"));
        Scan scanner = new Scan();
        scanner.addFamily("family1".getBytes());
        ResultScanner resultScanner = tablesource.getScanner(scanner);  
        System.out.println("Loading data ...");
        
        for (Result result= resultScanner.next();result !=null;result = resultScanner.next())
          {      keys ++;
            Put put = new Put(keys.toString().getBytes());
        	    Get get = new Get(result.getRow());
        	    Result entireRow = tablesource.get(get); 
        	    byte [] value = entireRow.value();
        	    put.addColumn("tweets".getBytes(), "value".getBytes(), value);
        	    tablereceiver.put(put);        	   
        	    System.out.println(value);
          }
    }
}
