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

public class RecRun {

    public static void main(String[] args) throws IOException {
        String zookeeper = args[0];
        String approach = args[1];

        List<Activity> operations = new ArrayList<Activity>();
        
        RefreshContentData rcd = new RefreshContentData(zookeeper);
        // operations.add(rcd);
        
        DumpContentMetadata dcm = new DumpContentMetadata(zookeeper);
        operations.add(dcm);
        System.out.println(approach);
        if (approach.equals("tag")) {
          DumpContentTag dct = new DumpContentTag(zookeeper);
          operations.add(dct);
	}
	else if (approach.equals("lda")) {
	    ScriptActivity sa = new ScriptActivity("echo 'J$p1ter' | sudo -u hdfs -S mahout rowid -i /io/result/weight/tf-vectors -o /io/result/matrix");
            operations.add(sa);
	}

        for (Activity ac : operations) {
            ac.run();
        }
    }
}

