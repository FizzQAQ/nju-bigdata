import ncsa.hdf.hdf5lib.exceptions.HDF5Exception;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


import ncsa.hdf.object.Dataset;
import ncsa.hdf.object.FileFormat;
import ncsa.hdf.object.Group;
import ncsa.hdf.object.h5.H5File;

import java.io.IOException;

public class Preprocess {
    public static class PreprocessMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String inputPath =value.toString();
            read_H5(inputPath,context);
        }
        private void read_H5(String filePath,Context context){
            FileFormat fileFormat = FileFormat.getFileFormat(FileFormat.FILE_TYPE_HDF5);
            H5File h5File = null;
            try {
                h5File = (H5File) fileFormat.open(filePath, FileFormat.READ);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            try {
                h5File.open();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            Group root = (Group) ((javax.swing.tree.DefaultMutableTreeNode) h5File.getRootNode()).getUserObject();
            try {
                extractAttributes(root, context);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            try {
                h5File.close();
            } catch (HDF5Exception e) {
                throw new RuntimeException(e);
            }
        }
        private void extractAttributes(Group group,Context context) throws Exception {
            String songId = getDatasetValue(group, "/metadata/songs", "song_id");
            String trackId = getDatasetValue(group, "/analysis/songs", "track_id");
            String title = getDatasetValue(group, "/metadata/songs", "title");
            String artistName = getDatasetValue(group, "/metadata/songs", "artist_name");
            String year = getDatasetValue(group, "/musicbrainz/songs", "year");
            String duration = getDatasetValue(group, "/analysis/songs", "duration");
            String tempo = getDatasetValue(group, "/analysis/songs", "tempo");
            String attributesStr = String.join(",",songId, trackId, title, artistName, year, duration, tempo);
            context.write(new Text("attributes"), new Text(attributesStr));
        }
        private String getDatasetValue(Group group, String groupName, String datasetName) throws Exception {
            Dataset dataset = (Dataset) group.getFileFormat().get(groupName + "/" + datasetName);
            dataset.init();
            Object data = dataset.read();
            if (data instanceof byte[]) {
                return new String((byte[]) data, "UTF-8");
            } else if (data instanceof int[]) {
                return Integer.toString(((int[]) data)[0]);
            } else if (data instanceof float[]) {
                return Float.toString(((float[]) data)[0]);
            } else if (data instanceof double[]) {
                return Double.toString(((double[]) data)[0]);
            }
            return data.toString();
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
