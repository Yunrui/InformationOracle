package io.rec;

import java.io.IOException;
import java.io.*;
import java.util.*;
import java.net.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

public class RefreshContentData extends Activity {
    private Configuration conf = HBaseConfiguration.create();

    public RefreshContentData(String zookeeper) {
        conf.set("hbase.zookeeper.quorum", zookeeper);
        conf.set("hbase.zookeeper.property.clientPort", "2181");
    }

    public void doCore() throws IOException {
        // clear column family d 
        byte[] name = Bytes.toBytes("content");
        HBaseAdmin admin = new HBaseAdmin(conf);
        HTableDescriptor table = admin.getTableDescriptor(name);
        if (table.hasFamily(Bytes.toBytes("d"))) {
            table.removeFamily(Bytes.toBytes("d"));
            admin.disableTable(name);
            admin.modifyTable(name, table);
            admin.enableTable(name);
        }

        HColumnDescriptor desc = new HColumnDescriptor("d");
        table.addFamily(desc);
        admin.disableTable(name);
        admin.modifyTable(name, table);
        admin.enableTable(name);
        admin.close();
    }

    public String getName() {
        return "RefreshContentData";
    }
}

