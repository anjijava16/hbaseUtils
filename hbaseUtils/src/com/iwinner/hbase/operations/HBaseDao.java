package com.iwinner.hbase.operations;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configuration;

public class HBaseDao {

    private static final String DICTIONARY_TABLE = "mytable";
    private static final String DICTIONARY_CF = "mycf";
    private static final String DICTIONARY_CF_TYPE = "type";

    private final Configuration conf;

    public HBaseDao() throws IOException {
        Configuration config = HBaseConfiguration.create();
        config.clear();
        config.set("hbase.zookeeper.quorum", "localhost");
        config.set("hbase.zookeeper.property.clientPort","2181");
        config.set("hbase.master", "localhost:60000");
        config.addResource(new Path("/usr/local/hbase/hbase/conf/hbase-site.xml"));
        this.conf = config;
    }

    public void addNameToTable(HTableInterface table, String name, String type) throws IOException {
        Put p = new Put(Bytes.toBytes(name)); 
        p.add(Bytes.toBytes(DICTIONARY_CF),
                  Bytes.toBytes(DICTIONARY_CF_TYPE),
                  Bytes.toBytes(type));
        System.out.println("About to put");
        table.put(p);
    }


    public void loadDictionaryIntoTable() throws IOException {
        HConnection connection = HConnectionManager.createConnection(this.conf);
        HTableInterface table = connection.getTable(DICTIONARY_TABLE);
         try {
            // Use the table as needed, for a single operation and a single thread
             String s = "TestName2";
             String t = "TestType2";
             addNameToTable(table,s,t);
         } finally {
        System.out.println("Got here");  
           table.close();
           connection.close();
         }
    }

    public static void main(String[] args) throws IOException {

        HBaseDao dao = new HBaseDao();
        dao.loadDictionaryIntoTable();
    }
}