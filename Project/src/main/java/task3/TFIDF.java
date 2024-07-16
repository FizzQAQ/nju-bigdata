package task3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;

public class TFIDF {
    public static class Mapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String valString = value.toString();
            String trackID = valString.split(",")[0];
            String part2 = valString.split("\\[")[1].split("\\]")[0];
            String[] words = part2.split(",");
            int totalWords = 0;
            for (String word : words) {
                int count = Integer.parseInt(word.split(":")[1]);
                totalWords += count;
            }
            for (String word : words) {
                String wordID = word.split(":")[0];
                int count = Integer.parseInt(word.split(":")[1]);
                double tf = (double) count / totalWords;
                context.write(new Text(wordID), new Text(trackID + ":" + tf));
            }
        }
    }

    public static class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text> {
        private int trackCount;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            trackCount = context.getConfiguration().getInt("trackCount", 1);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            int totalTracks = 1;
            List<Text> valuesList = new ArrayList<>();
            for (Text value : values) {
                totalTracks++;
                valuesList.add(new Text(value));
            }
            for (Text value : valuesList) {
                String[] parts = value.toString().split(":");
                String trackID = parts[0];
                double tf = Double.parseDouble(parts[1]);
                double idf = Math.log10((double) trackCount / totalTracks);
                double tfidf = tf * idf;
                context.write(new Text(trackID + "," + key.toString() + ":" + tfidf), new Text());
            }
        }
    }
}
