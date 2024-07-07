package task2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class GenreMining {


    public static class GenreInformationMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit file = (FileSplit) context.getInputSplit();
            String fileName = file.getPath().getName();
            if (fileName.equals("genres.txt"))  // genres文件中 写入 track_id 和 genres(进行标记)
            {
                String[] temp = value.toString().split(",");
                context.write(new Text(temp[0]), new Text("@genre:" + temp[1]));
            } else if (fileName.equals("songs.txt"))  // songs文件中 写入 track_id 和各个信息(进行标记)
            {
                String[] temp = value.toString().split(",");
                context.write(new Text(temp[1]), new Text("@energy:" + temp[7]));    //energy
                context.write(new Text(temp[1]), new Text("@tempo:" + temp[8]));    //tempo
                context.write(new Text(temp[1]), new Text("@loudness:" + temp[9]));    //loudness
                context.write(new Text(temp[1]), new Text("@duration:" + temp[10]));    //duration
                context.write(new Text(temp[1]), new Text("@danceability:" + temp[11]));    //danceability
            }
        }
    }

    public static class GenreInformationReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String energy = "";
            String tempo = "";
            String loudness = "";
            String duration = "";
            String danceability = "";
            String genre = "";
            for (Text value : values) {
                String s = value.toString();
                if (s.startsWith("@genre:")) {
                    genre = s.substring(7);
                } else if (s.startsWith("@energy:")) {
                    energy = s.substring(8);
                } else if (s.startsWith("@tempo:")) {
                    tempo = s.substring(7);
                } else if (s.startsWith("@loudness:")) {
                    loudness = s.substring(10);
                } else if (s.startsWith("@duration:")) {
                    duration = s.substring(10);
                } else if (s.startsWith("@danceability:")) {
                    danceability = s.substring(14);
                }
            }
            if (!genre.isEmpty()) {
                context.write(new Text(genre), new Text(energy + "," + tempo + "," + loudness + "," + duration + "," + danceability));
            }
        }
    }


    public static class GenreMiningMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] temp = value.toString().split(",", 2);
            context.write(new Text(temp[0]), new Text(temp[1]));
        }
    }

    public static class GenreMiningReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            final int[] cnt = {0};
            double[] sums = StreamSupport.stream(values.spliterator(), false)
                    .map(Text::toString)
                    .peek(s -> cnt[0]++)
                    .map(s -> s.split(","))
                    .map(s -> Arrays.stream(s).mapToDouble(Double::parseDouble).toArray())
                    .reduce(new double[5], (acc, parts) -> {
                        for (int i = 0; i < parts.length; i++) {
                            acc[i] += parts[i];
                        }
                        return acc;
                    });
            String res = "";
            for (double sum : sums) {
                sum /= cnt[0];
                res += String.format("%.3f", sum);
                res += ",";
            }
            context.write(new Text(key.toString()), new Text(res.substring(0, res.length() - 1)));
        }
    }
}


