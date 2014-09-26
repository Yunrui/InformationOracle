Register '/tmp/DM/myUDF.py' using jython as myfuncs;
Log = LOAD 'hbase://log' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('m:action m:id m:timestamp', '-loadKey true') AS (userid:chararray, action:chararray, doc:chararray, time:chararray);
Log = foreach Log generate myfuncs.split(userid, '|@@@@|', 0) as user, action, doc, ((double)(myfuncs.split(userid, '|@@@@|', 1))) as (time:double);
Content = LOAD 'hbase://content' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('m:title m:url m:time m:topic_1 m:topic_2 m:topic_3 m:topic_4 m:topic_5  d:*', '-loadKey true') AS (content:chararray, title:chararray, url:chararray, time:chararray, topic_1:chararray, topic_2:chararray, topic_3:chararray, topic_4:chararray, topic_5:chararray, tag:map[]);
Content = foreach Content generate content, title, url, time, topic_1, topic_2, topic_3, topic_4, topic_5;

J = join Log by doc, Content by content;
K = foreach J generate user, action, doc, Log::time as time,  topic_1, topic_2, topic_3, topic_4, topic_5;
G = group K by (user, doc);
F = foreach G {
      Opened = filter K by action matches 'open';
        Opened_order = order Opened by time ASC;
          Opened_top = limit Opened_order 1;
            Others = filter K by action != 'open';
              Others_order = order Others by time ASC;
                Others_top = limit Others_order 1;

                  generate FLATTEN(((COUNT(Others_top) > 0) ? Others_top : Opened_top));
}; 

P = foreach F generate user, action, doc, time, FLATTEN(myfuncs.generateTopics(topic_1, topic_2, topic_3, topic_4, topic_5)) as (topic, weight);

AdjustWeight = foreach P generate user, topic, (action == 'favorit' ? 5 : (action == 'dislike' ? -5 : 1)) * weight as weight;
TopicPrep = group AdjustWeight by (user, topic);
TopicWeight = foreach TopicPrep generate FLATTEN(group) as (user, topic), SUM(AdjustWeight.weight) as weight;
UserTopicWeight = filter TopicWeight by weight > 0;

ContentTopicMap = foreach Content generate content, title, url, time, FLATTEN(myfuncs.generateTopics(topic_1, topic_2, topic_3, topic_4, topic_5)) as (topic, cWeight);

UserContent = join UserTopicWeight by topic, ContentTopicMap by topic;
UserContent = foreach UserContent generate user, content, title, url, time, weight * cWeight as weight;

UserContentGroup = group UserContent by (user, content, title, url, time);
M= foreach UserContentGroup generate FLATTEN(group) as (user, content, title, url, time), SUM(UserContent.weight) as weight;
L=  group M by user;
Path5 = foreach L{
            orderList = order M by weight DESC;
                    topN = limit orderList 20;
                            generate FLATTEN(topN);
};
store Path5 into 'hdfs://v3namenode:8020/tmp/newsletter' using PigStorage('\t');

