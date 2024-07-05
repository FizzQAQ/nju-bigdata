import ncsa.hdf.hdf5lib.H5;
import ncsa.hdf.hdf5lib.HDF5Constants;
import ncsa.hdf.hdf5lib.exceptions.HDF5Exception;
import ncsa.hdf.hdf5lib.exceptions.HDF5LibraryException;

public class test {
    public static void main(String[] args) {
        String filePath="/home/fizz/littledataset/songs/A/A/A/TRAAAAW128F429D538.h5";
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
        System.out.println(year);


    }
}
