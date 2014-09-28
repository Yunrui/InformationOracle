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

public class DumpContentMetadata extends Activity {
    private Configuration conf = HBaseConfiguration.create();
    
    public DumpContentMetadata(String zookeeper) {
        conf.set("hbase.zookeeper.quorum", zookeeper);
	conf.set("hbase.zookeeper.property.clientPort", "2181");
    }

    public void doCore() throws IOException {
        byte[] name = Bytes.toBytes("content");
        HBaseAdmin admin = new HBaseAdmin(conf);
        HTableDescriptor td = admin.getTableDescriptor(name);
        if (!td.hasFamily(Bytes.toBytes("m"))) {
            HColumnDescriptor desc = new HColumnDescriptor("m");
            td.addFamily(desc);
            admin.disableTable(name);
            admin.modifyTable(name, td);
            admin.enableTable(name);
        }

        HTable table = new HTable(conf, "content");
        try {
            Path path = new Path("hdfs://localhost:8020/io/meta/dump");
            FileSystem fs = FileSystem.get(new URI("hdfs://localhost:8020"), new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
            String line;
            line = br.readLine();
            while (line != null) {
                String[] parts = line.split("\\|@@@@\\|");
		String url = parts[1];
		String time = parts[2];
		String title = parts[3];
		String key = "/" + parts[4];

		table.put(new Put(Bytes.toBytes(key)).add(Bytes.toBytes("m"), Bytes.toBytes("title"), Bytes.toBytes(title)));
		table.put(new Put(Bytes.toBytes(key)).add(Bytes.toBytes("m"), Bytes.toBytes("url"), Bytes.toBytes(url)));
		table.put(new Put(Bytes.toBytes(key)).add(Bytes.toBytes("m"), Bytes.toBytes("time"), Bytes.toBytes(time)));

		line = br.readLine();
            }
        }
        catch(URISyntaxException e) {
        }
    }

    public String getName() {
        return "DumpContentMetadata";
    }
}

