Register '/tmp/DM/myUDF.py' using jython as myfuncs;
User = LOAD 'hbase://user' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('t:*', '-loadKey true') AS (user:chararray, interest:map[]);
User = foreach User generate user, FLATTEN(myfuncs.expandMap(interest)) AS (tag:chararray, important:double);
Content = LOAD 'hbase://content' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('m:title m:url m:time d:*', '-loadKey true') AS (content:chararray, title:chararray, url:chararray, time:chararray, tag:map[]);
Content = foreach Content generate content, title, url, time, FLATTEN(myfuncs.expandMap(tag)) AS (tag:chararray, frequency:double);
Crossed = cross User, Content;
Path = filter Crossed by myfuncs.fuzzySame(User::tag, Content::tag);
Path1 = foreach Path generate user, content, User::tag as tag, frequency, title, url, time;
Path2 = group Path1 by (user, content, title, url, time);

Path3 = foreach Path2 generate FLATTEN(group) as (user, content, title, url, time), SUM(Path1.frequency) as weight;
Path4 = group Path3 by user;
Path5 = foreach Path4 {
        orderList = order Path3 by weight DESC;
            topN = limit orderList 50;
                generate FLATTEN(topN);
};

store Path5 into 'hdfs://v3namenode:8020/tmp/newsletter' using PigStorage('\t');

