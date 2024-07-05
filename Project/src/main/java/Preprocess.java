
import ncsa.hdf.hdf5lib.exceptions.HDF5Exception;
import ncsa.hdf.hdf5lib.exceptions.HDF5LibraryException;
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

import ncsa.hdf.hdf5lib.*;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
            System.out.println(paths.length);
            for(Path path :paths){
                System.out.println(path.toString());
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
            String inputPath =value.toString();
            read_H5(inputPath,context);
        }
        private void read_H5(String filePath,Context context){
            int fileId = 0;
            int datasetid=0;
            int year;
            try {
                fileId = H5.H5Fopen(filePath, HDF5Constants.H5F_ACC_RDONLY, HDF5Constants.H5P_DEFAULT);
                int[] data = new int[1];
                datasetid= H5.H5Dopen(fileId, "/musicbrainz/songs/year");
                try {
                    H5.H5Dread(datasetid, HDF5Constants.H5T_NATIVE_INT, HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL, HDF5Constants.H5P_DEFAULT, data);
                    year=data[0];
                } catch (HDF5Exception e) {
                    throw new RuntimeException(e);
                }

                H5.H5Dclose(datasetid);;
            } catch (HDF5LibraryException e) {
                throw new RuntimeException(e);
            }
            try {
                context.write(new Text(String.valueOf(year)),new Text(""));
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }


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
