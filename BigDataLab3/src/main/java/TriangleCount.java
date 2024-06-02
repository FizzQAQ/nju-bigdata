import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TriangleCount {

    public static class TriangleCountMapper extends Mapper<Object, Text, Text, Text> {

    }

    public static class TriangleCountReducer extends Reducer<Text, Text, Text, Text> {
    }
}
