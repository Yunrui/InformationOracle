import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Comparator;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.LinkedList;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader.Option;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.VectorWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.SequentialAccessSparseVector;

public class SequenceFileOperator {
    private Configuration conf = new Configuration();

    public SequenceFileOperator() {
        this.conf.setStrings("dfs.namenode.adress", "192.168.59.103:8020");
    }

    public static void main(String[] args) throws IOException {
        SequenceFileOperator docToSeqFileWriter = new SequenceFileOperator();
        String dictionaryFilePath = args[0];
        String sequenceFilePath = args[1];

        Map<String, String> dic = docToSeqFileWriter.readDictionary(dictionaryFilePath);

        docToSeqFileWriter.dumpToHBase(dic, sequenceFilePath);
    }

      private Map<String, String> readDictionary(String sequenceFilePath) throws IOException {
        Option filePath = SequenceFile.Reader.file(new Path(sequenceFilePath));
        SequenceFile.Reader sequenceFileReader = new SequenceFile.Reader(conf, filePath);
        Writable key = (Writable) ReflectionUtils.newInstance(sequenceFileReader.getKeyClass(), conf);
        Writable value = (Writable) ReflectionUtils.newInstance(sequenceFileReader.getValueClass(), conf);

        Map<String, String> dic = new HashMap<String, String>();

        try {
          while (sequenceFileReader.next(key, value)) {
            dic.put(value.toString(), key.toString());
          }
        } finally {
          IOUtils.closeStream(sequenceFileReader);
        }

        return dic;
      }

      private void dumpToHBase(Map<String, String> dictionary, String sequenceFilePath) throws IOException {
        Option filePath = SequenceFile.Reader.file(new Path(sequenceFilePath));
        SequenceFile.Reader sequenceFileReader = new SequenceFile.Reader(conf, filePath);
        Writable key = (Writable) ReflectionUtils.newInstance(sequenceFileReader.getKeyClass(), conf);
        Writable value = (Writable) ReflectionUtils.newInstance(sequenceFileReader.getValueClass(), conf);


        try {
          while (sequenceFileReader.next(key, value)) {

            /*
            if (!key.toString().contains("highscalability")) {
              continue;
            }
            */

            VectorWritable vector = (VectorWritable) value;

            Map<String, Double> doc = new HashMap<String, Double>();
            for (Iterator<Vector.Element> iter = ((SequentialAccessSparseVector)vector.get()).iterator(); iter.hasNext(); ) {
              Vector.Element element = iter.next();

              String feature = dictionary.get(String.valueOf(element.index()));
              Double frequency = element.get();

              if (frequency > 0 && !feature.toString().contains("'")) {
                doc.put(feature, frequency);

              }
            }

           int i = 0;
           List orderedDoc = new LinkedList(sortByValue(doc).entrySet());
           for(Iterator it = orderedDoc.iterator(); it.hasNext();) {
             Map.Entry entry = (Map.Entry) it.next();

             if (entry.getValue().toString().contains("\n"))
               continue;

             System.out.printf("put 'content', '%s', 'd:%s', %f\n", key, entry.getKey(), entry.getValue());

             if (++i >= 20) {
               break;
             }
           }

          }
        } finally {
          IOUtils.closeStream(sequenceFileReader);
        }
      }

      static Map sortByValue(Map map) {
        List list = new LinkedList(map.entrySet());
        Collections.sort(list, new Comparator() {
          public int compare(Object o1, Object o2) {
            return -((Comparable) ((Map.Entry) (o1)).getValue()).compareTo(((Map.Entry) (o2)).getValue());
          }
        });

        Map result = new LinkedHashMap();
        for (Iterator it = list.iterator(); it.hasNext();) {
          Map.Entry entry = (Map.Entry)it.next();
          result.put(entry.getKey(), entry.getValue());
        }
        return result;
      }
}
