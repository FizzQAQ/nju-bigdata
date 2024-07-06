package task2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.PriorityQueue;

//定义新类 SongCount 方便放入堆中统计最受欢迎的十首歌曲
class SongCount implements Comparable<SongCount> {
    private String title;
    private int playCount;

    public SongCount(String title, int playCount) {
        this.title = title;
        this.playCount = playCount;
    }

    public String getTitle() {
        return title;
    }

    public int getPlayCount() {
        return playCount;
    }

    @Override
    public int compareTo(SongCount other) {
        return Integer.compare(this.playCount, other.playCount);
    }
}

public class MostPopular {

    public static class MostPopularMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit file = (FileSplit) context.getInputSplit();
            String fileName = file.getPath().getName();
            if (fileName.equals("users.txt"))  // users文件中 写入 song_id 和 play_count
            {
                String[] temp = value.toString().split(",");
                context.write(new Text(temp[1]), new Text(temp[2]));
            } else if (fileName.equals("songs.txt"))  // songs文件中 写入 song_id 和 tittle(进行标记)
            {
                String[] temp = value.toString().split(",");
                context.write(new Text(temp[0]), new Text("@title:" + temp[2]));
            }
        }
    }

    public static class MostPopularReducer extends Reducer<Text, Text, Text, Text> {
        // 优先队列实现最小堆
        private final PriorityQueue<SongCount> minHeap = new PriorityQueue<>(10);

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String title = "";
            int cnt = 0;
            for (Text value : values) {
                String s = value.toString();
                if (s.startsWith("@title:")) // 检测标记，赋值 title
                {
                    title = s.substring(7);
                } else // 统计播放次数
                {
                    cnt += Integer.parseInt(s);
                }
            }
            // 创建实例，歌曲的名字-播放次数，并放入最小堆中
            SongCount songCount = new SongCount(title, cnt);
            if (minHeap.size() < 10) {
                minHeap.offer(songCount);
            } else if (songCount.compareTo(minHeap.peek()) > 0) {
                minHeap.poll();
                minHeap.offer(songCount);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            //写入最小堆中的10首最受欢迎的歌曲
            while (!minHeap.isEmpty()) {
                SongCount songCount = minHeap.poll();
                context.write(new Text(songCount.getTitle()), new Text(String.valueOf(songCount.getPlayCount())));
            }
        }
    }


}


