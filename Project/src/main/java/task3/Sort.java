package task3;

import java.io.IOException;

import org.apache.hadoop.io.Text;

public class Sort {
    public static class Mapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String valString = value.toString();
            context.write(new Text(valString.split(",")[0]), new Text(valString.split(",")[1]));
        }
    }

    public static class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String valString = "";
            for (Text value : values) {
                valString += value.toString() + ",";
            }
            context.write(new Text(key.toString() + ";" + valString.substring(0, valString.length() - 1)), new Text());
        }
    }
}
