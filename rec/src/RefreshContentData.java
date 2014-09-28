import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

public class RefreshContentData {

    public static void main(String[] args) throws IOException {
        String zookeeper = args[0];
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", zookeeper);
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        
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
}

