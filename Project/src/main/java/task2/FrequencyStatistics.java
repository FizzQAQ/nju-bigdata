package task2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;


public class FrequencyStatistics {
    public static class GenreLyricsMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit file = (FileSplit) context.getInputSplit();
            String fileName = file.getPath().getName();
            if (fileName.equals("genres.txt"))  // genres文件中 写入 track_id 和 genres(进行标记)
            {
                String[] temp = value.toString().split(",");
                context.write(new Text(temp[0]), new Text("@genre:" + temp[1]));
            } else if (fileName.equals("lyrics.txt"))  // lyrics文件中 写入 track_id 和 lyrics_cnt(进行标记)
            {
                String[] temp = value.toString().split(",\\[");
                temp[1] = temp[1].substring(0, temp[1].length() - 1);
                context.write(new Text(temp[0]), new Text(temp[1]));
            }
        }
    }

    public static class GenreLyricsReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<String> lyrics = new ArrayList<>();
            String genre = "";
            for (Text value : values) {
                String s = value.toString();
                if (s.startsWith("@genre:"))  // 检测到genre标记，设置genre值
                {
                    genre = s.substring(7);
                } else {
                    lyrics.add(s);
                }
            }
            if (!genre.isEmpty())  // 若此track有genre记录，则写入其genre-lyric键值对
            {
                for (String lyric : lyrics) {
                    context.write(new Text(genre), new Text(lyric));
                }
            }
        }
    }

    public static class FrequencyStatisticsMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] temp = value.toString().split(",", 2); // limit设置为2,只分割一次即可
            context.write(new Text(temp[0]), new Text(temp[1]));
        }
    }

    public static class FrequencyStatisticsReducer extends Reducer<Text, Text, Text, Text> {

        private MultipleOutputs<Text, Text> mos;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            mos = new MultipleOutputs<>(context);
        }

        private int maxGenreCnt = 0;
        private String maxGenreName = "";

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            final int[] cnt = {0};
            // 流操作
            Stream<Map.Entry<String, Integer>> combinedStream = StreamSupport.stream(values.spliterator(), false).map(Text::toString)    // Text转为int
                    .peek(s -> cnt[0]++)    // 计数歌曲数量
                    .flatMap(input -> Arrays.stream(input.split(",")) // 扁平化，将string流先按","分割后合并
                            .map(pair -> {
                                String[] parts = pair.split(":"); // 按:进行分割
                                return new AbstractMap.SimpleEntry<>(parts[0], Integer.parseInt(parts[1]));
                            })  //  映射，将字符串映射为map的键值对
                    );
            // 将合并流转为Map,合并方法用sum求和即可
            Map<String, Integer> mergedMap = combinedStream.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, Integer::sum));

            // 更新歌曲数最多的流派
            if (cnt[0] > maxGenreCnt) {
                maxGenreCnt = cnt[0];
                maxGenreName = key.toString();
            }
            // 将键值对 词-词频 写入文件
            for (Map.Entry<String, Integer> e : mergedMap.entrySet()) {
                mos.write(new Text(e.getKey()), new Text(String.valueOf(e.getValue())), key.toString());
            }

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
            String[] genres = {"Rock", "Metal", "Pop", "Country", "Rap", "Electronic", "Reggae", "Punk", "RnB", "Jazz", "Blues", "Folk", "Latin", "World", "NewAge"};
            // 将计数器更新，值为流派在数组中的下标
            for (int i = 0; i < genres.length; i++) {
                if (maxGenreName.equals(genres[i])) {
                    context.getCounter("genre", "max").setValue(i);
                }
            }
        }

    }
}
