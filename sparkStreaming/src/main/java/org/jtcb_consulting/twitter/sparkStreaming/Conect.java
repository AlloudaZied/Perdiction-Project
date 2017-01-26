//package org.jtcb_consulting.twitter.sparkStreaming;
//
//import scala.annotation.serializable;
//
//public class Conect extends serializable {
////	private static Connection connection = null ;
////	private  Conect() throws IOException
////	{
////		getCon();
////		
////	}
////	static Table table = null ;
////	private  static Connection getCon() throws IOException
////	{
////		 Configuration configuration = new Configuration();
////         configuration.set("zookeeper.znode.parent", "/hbase-unsecure");
////         connection = ConnectionFactory.createConnection(configuration);
////		return connection ;
////	}
////	public static void putToHbase(String val) throws IOException
////	{ 
////		table = getCon().getTable(TableName.valueOf("Desc"));
////		Integer x = new Integer((int)Math.random());						
////		Put put = new Put(x.toString().getBytes());
////		put.addColumn("descripteur".getBytes(), "value".getBytes(), val.getBytes());
////		table.put(put);
////		
////	}
//	
//}
