package com.iwinner.hbase.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;


public class RetriveData{

   public static void main(String[] args) throws IOException, Exception{
   
	   try{
	   
		Configuration conf = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(conf);
/**/
      // Instantiating Configuration class
      Configuration config = HBaseConfiguration.create();
      /*config.set("fs.defaultFS", "hdfs://localhost:8020");
      config.set("hbase.master", "http://localhost:60000"); 
      config.set("hbase.zookeeper.property.clientPort", "2181");
      */
      
      config.set("hbase.zookeeper.quorum", "localhost");
      config.set("hbase.zookeeper.property.clientPort","2181");
      config.set("hbase.master", "localhost:60000");
      // Instantiating HTable class
      
      
      //HTable table = new HTable(config, "emp");


		TableName tblName = TableName.valueOf("emp");
		Table table = connection.getTable(tblName);
		
		System.out.println("Connection info is here ===>>>"+connection.getConfiguration());
		System.out.println("Table INfo is here ===>>"+table.getName().getQualifier());
      // Instantiating Get class
      Get g = new Get(Bytes.toBytes("row1"));

      // Reading the data
      Result result = table.get(g);

      System.out.println("Result is "+result);
      // Reading values from Result class object
      byte [] value = result.getValue(Bytes.toBytes("personal data"),Bytes.toBytes("name"));

      byte [] value1 = result.getValue(Bytes.toBytes("personal data"),Bytes.toBytes("city"));

      // Printing the values
      String name = Bytes.toString(value);
      String city = Bytes.toString(value1);
      
      System.out.println("name: " + name + " city: " + city);
   }catch(Exception e){
	   e.printStackTrace();
   }
   }
}