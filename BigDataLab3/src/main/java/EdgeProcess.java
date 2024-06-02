import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class EdgeProcess {


    public static class EdgeProcessMapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] websiteId = value.toString().split("\40");
            String edge = null;
            if (websiteId[0].compareTo(websiteId[1]) < 0) {
                edge = websiteId[0] + " " + websiteId[1];
            } else if (websiteId[0].compareTo(websiteId[1]) > 0) {
                edge = websiteId[1] + " " + websiteId[0];
            }
            context.write(new Text(edge), new Text(""));
        }
    }

    public static class EdgeProcessReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int cnt = 0;
            for (Text val : values) {
                cnt++;
            }
            if (cnt == 2) {
                context.write(key, new Text(""));
            }
        }
    }
}
