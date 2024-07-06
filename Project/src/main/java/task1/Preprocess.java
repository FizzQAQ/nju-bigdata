package task1;

import io.jhdf.HdfFile;
import io.jhdf.api.Dataset;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.hadoop.fs.FSDataInputStream;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Preprocess {
    public static class PreprocessFileInputFormat extends FileInputFormat<NullWritable, Text> {
        @Override
        protected boolean isSplitable(JobContext context, Path filename) {
            return false;
        }

        @Override
        public List<InputSplit> getSplits(JobContext job) throws IOException {
            List<InputSplit> splits = new ArrayList<>();
            FileSystem fs = FileSystem.get(job.getConfiguration());
            Path[] paths = FileInputFormat.getInputPaths(job);

            for (Path path : paths) {
                addFilesRecursively(fs, path, splits);
            }
            return splits;
        }

        private void addFilesRecursively(FileSystem fs, Path path, List<InputSplit> splits) throws IOException {
            FileStatus[] fileStatuses = fs.listStatus(path);

            for (FileStatus fileStatus : fileStatuses) {
                if (fileStatus.isDirectory()) {
                    addFilesRecursively(fs, fileStatus.getPath(), splits);
                } else {
                    splits.add(new FileSplit(fileStatus.getPath(), 0, fileStatus.getLen(), null));
                }
            }
        }

        public RecordReader<NullWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            return new PreprocessFileRecordReader();
        }
    }

    public static class PreprocessFileRecordReader extends RecordReader<NullWritable, Text> {
        private FileSplit fileSplit;
        private Text currentFilePath = new Text();
        private boolean processed = false;

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            this.fileSplit = (FileSplit) split;
            this.currentFilePath.set(fileSplit.getPath().toString());
        }

        @Override
        public boolean nextKeyValue() throws IOException {
            if (!processed) {
                processed = true;
                return true;
            }
            return false;
        }

        @Override
        public NullWritable getCurrentKey() {
            return NullWritable.get();
        }

        @Override
        public Text getCurrentValue() {
            return currentFilePath;
        }

        @Override
        public float getProgress() {
            return processed ? 1.0f : 0.0f;
        }

        @Override
        public void close() throws IOException {
            // No resources to close
        }
    }

    public static class PreprocessMapper extends Mapper<NullWritable, Text, Text, Text> {
        @Override
        public void map(NullWritable key, Text value, Context context) throws IOException, InterruptedException {
            String path = value.toString();
            String out = "";
            String songId = look_up(path, "/metadata/songs", "song_id");
            out += look_up(path, "/analysis/songs", "track_id") + ",";
            out += look_up(path, "/metadata/songs", "title") + ",";
            out += look_up(path, "/metadata/songs", "release") + ",";
            out += look_up(path, "/metadata/songs", "artist_id") + ",";
            out += look_up(path, "/metadata/songs", "artist_name") + ",";
            out += look_up(path, "/analysis/songs", "mode")+ ",";
            out += look_up(path, "/analysis/songs", "energy")+ ",";
            out += look_up(path, "/analysis/songs", "tempo")+ ",";
            out += look_up(path, "/analysis/songs", "loudness")+ ",";
            out += look_up(path, "/analysis/songs", "duration") + ",";
            out += look_up(path, "/analysis/songs", "danceability")+ ",";
            out += look_up(path, "/musicbrainz/songs", "year") ;
            context.write(new Text(songId), new Text(out));
        }

        public String look_up(String path, String Att, String target) {
            String out = "";
            File tempFile = null;
            try {
                FileSystem fs = FileSystem.get(new Configuration());
                Path p = new Path(path);
                FSDataInputStream inputStream = fs.open(p);
                tempFile = File.createTempFile("tempHdf5File", ".h5");
                try (FileOutputStream fileOutputStream = new FileOutputStream(tempFile)) {
                    byte[] buffer = new byte[10240];
                    int bytesRead;
                    while ((bytesRead = inputStream.read(buffer)) != -1) {
                        fileOutputStream.write(buffer, 0, bytesRead);
                    }
                }
                HdfFile hdfFile = new HdfFile(tempFile);
                Dataset dataset = hdfFile.getDatasetByPath(Att);
                Object data = dataset.getData();
                if (data instanceof LinkedHashMap) {
                    LinkedHashMap<String, Object> mapdata = (LinkedHashMap<String, Object>) data;
                    for (Map.Entry<String, Object> entry : mapdata.entrySet()) {
                        Object v = entry.getValue();
                        if (entry.getKey().equals(target)) {

                            if (v instanceof int[]) {
                                out = String.valueOf(((int[]) v)[0]);
                            } else if (v instanceof String[]) {
                                out = ((String[]) v)[0];
                            } else if (v instanceof float[]) {
                                out = String.valueOf(((float[]) v)[0]);
                            } else if (v instanceof double[]) {
                                out = String.valueOf(((double[]) v)[0]);
                            }
                        }
                    }
                }

            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                if (tempFile != null && tempFile.exists()) {
                    tempFile.delete();
                }
            }
            return out;
        }
    }

    public static class PreprocessReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                context.write(key, val);
            }
        }
    }
}
