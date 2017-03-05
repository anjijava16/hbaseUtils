package com.iwinner.hbase.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;


// Start the Hbase : start-hbase.sh (Hbase internally start the ZooKeper Server)
// start the Hbase shell : hbase shell

/**


 Create:===>>create 'emp', 'personal data', 'professional data'
		 
		 ==================================================================================
		 Read/View:==>>list
		 
		 ==================================================================================
		 Insert/Update(Put):==>>
		 
		 Syntax: put ’<table name>’,’row1’,’<colfamily:colname>’,’<value>’
		  put 'emp','1','personal data:name','raju'
		  put 'emp','1','personal data:city','hyderabad'
		 put 'emp','1','professional data:designation','manager'
		 put 'emp','1','professional data:salary','50000'
		 
		 
		 put 'emp','2','personal data:name','raju'
		  put 'emp','2','personal data:city','Chenni'
		 put 'emp','2','professional data:designation','Arch manager'
		 put 'emp','2','professional data:salary','60000'
		  
		 Update:  put 'emp','row1','personal:city','Delhi'
		 ==================================================================================
		 get 'emp', '1'  // Read the specific reacord data
		 Syntax: delete ‘<table name>’, ‘<row>’, ‘<column name >’, ‘<time stamp>’
		 
		 
		 ==================================================================================
		 
		 
		 deleteall ‘<table name>’, ‘<row>’,
		 delete 'emp', '1', 'personal data:city',

*/

public class HbaseUtils {

	 private static Configuration conf = null;
	    /**
	     * Initialization
	     */
	 public static Configuration hbaseConnection(){
		    conf = HBaseConfiguration.create();
	        conf.set("hbase.zookeeper.quorum", "localhost");
			conf.set("hbase.zookeeper.property.clientPort","2181");
			conf.set("hbase.master", "localhost:60000");
			return conf;

	 }
	 
}
