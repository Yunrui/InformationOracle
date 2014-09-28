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
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hbase");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        /*
        HTable table = new HTable(conf, "test3");
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);

        for (Result res : scanner) {
            System.out.println(res.toString() + "----" + Bytes.toString(res.value()));
        }

        scanner.close();
        table.close();
        */

        HBaseAdmin admin = new HBaseAdmin(conf);
        admin.deleteColumn(Bytes.toBytes("test3"), "d");
    }
}

