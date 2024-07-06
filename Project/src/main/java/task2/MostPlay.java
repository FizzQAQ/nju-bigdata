package task2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

public class MostPlay {
    public static class MostPlayMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit file = (FileSplit) context.getInputSplit();
            String fileName = file.getPath().getName();
            if (fileName.equals("users.txt"))   // users文件中， 写入 user_id - play_count(进行标记) ， song_id - play_count(进行标记)
            {
                String[] temp = value.toString().split(",");
                context.write(new Text(temp[0]), new Text("@user:" + temp[2]));
                context.write(new Text(temp[1]), new Text("@song:" + temp[2]));
            } else if (fileName.equals("songs.txt"))  // songs文件中， 写入 song_id - artist_id(进行标记)
            {
                String[] temp = value.toString().split(",");
                context.write(new Text(temp[0]), new Text("@artist:" + temp[4]));
            }
        }
    }

    public static class MostPlayReducer extends Reducer<Text, Text, Text, Text> {

        private int userMaxCnt = 0;
        private String userMaxId = "";
        private int artistMaxCnt = 0;
        private String artistMaxId = "";

        private Map<String, Integer> map = new HashMap<>();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String artistId = "";
            int userCnt = 0;
            int artistCnt = 0;

            for (Text value : values) {
                String s = value.toString();
                if (s.startsWith("@user:")) // 检测标记 user 累加用户播放次数
                {
                    userCnt += Integer.parseInt(s.substring(6));
                } else if (s.startsWith("@song:")) // 检测标记 song 累加歌曲播放次数
                {
                    artistCnt += Integer.parseInt(s.substring(6));
                } else if (s.startsWith("@artist:")) // 检测标记 artist 赋值artistId
                {
                    artistId = s.substring(8);
                }
            }

            if (userCnt > userMaxCnt) // 与当前播放次数最多的用户进行比较
            {
                userMaxCnt = userCnt;
                userMaxId = key.toString();
            }

            if (!artistId.isEmpty()) // 统计艺术家播放次数
            {
                if (map.containsKey(artistId)) // 若表中存在则更新artistCnt
                {
                    artistCnt += map.get(artistId);
                }
                map.put(artistId, artistCnt); // 更新表

                if (artistCnt > artistMaxCnt) // 与当前被播放次数最多的艺术家进行比较
                {
                    artistMaxCnt = artistCnt;
                    artistMaxId = artistId;
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            //写入播放次数最多的用户和艺术家
            context.write(new Text(userMaxId), new Text(artistMaxId));
        }
    }
}


