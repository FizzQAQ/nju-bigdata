package task2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DurationStatistics {
    public static class DurationStatisticsMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] temp = value.toString().split(",");
            double duration = Double.parseDouble(temp[10]);
            int x = (int) duration / 60;    // 计算时长区间
            if (x > 9) {
                x = 9;
            }
            context.write(new Text(String.valueOf(x)), new Text("1"));
        }
    }

    public static class DurationStatisticsReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int l = 60 * Integer.parseInt(key.toString());
            int r = 60 + l;
            int cnt = 0;
            for (Text value : values) {
                cnt += Integer.parseInt(value.toString());
            }
            if (l == 540) {
                context.write(new Text("[" + l + ",~)"), new Text(String.valueOf(cnt)));
            } else {
                context.write(new Text("[" + l + "," + r + ")"), new Text(String.valueOf(cnt)));
            }
        }
    }

}
