package task3;

import java.io.IOException;

import org.apache.hadoop.io.Text;

public class TFIDF {
    public static class Mapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, Text> {
        private final static Text word = new Text();
        private final static Text doc = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            String[] docWord = line[0].split("@");
            word.set(docWord[1]);
            doc.set(docWord[0] + "=" + line[1]);
            context.write(word, doc);
        }
    }
}
