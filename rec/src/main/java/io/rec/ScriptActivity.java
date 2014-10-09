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

public class ScriptActivity extends Activity {
    private String cmdlet;
    
    public ScriptActivity(String cmdlet) {
        this.cmdlet = "echo 'hello'"; //cmdlet;
    }

    public void doCore() throws IOException {
        Process p = Runtime.getRuntime().exec(this.cmdlet);
	try {
	    p.waitFor();
            OutputStream os = p.getOutputStream();
	    StringBuffer sb = new StringBuffer();
	    // BufferedReader br = new BufferedReader(new InputStreamReader(os));
	    String buffer = null;
	}
	catch(InterruptedException e) {
	    System.out.println(e.getStackTrace());
	}

        System.out.println("Exit value = " + p.exitValue());
    }

    public String getName() {
        return "ScriptActivity - " + this.cmdlet;
    }
}

