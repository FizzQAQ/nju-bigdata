import io.jhdf.HdfFile;
import io.jhdf.api.Attribute;
import io.jhdf.api.Dataset;
import org.apache.commons.lang3.ArrayUtils;

import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;

public class TEST {
    public static void main(String[] args) {
        String out="";
        String path="/home/fizz/littledataset/songs/A/A/A/TRAAAAW128F429D538.h5";
        out+=look_up(path,"/metadata/songs","song_id")+",";
        out+=look_up(path,"/analysis/songs","track_id")+",";
        out+=look_up(path,"/metadata/songs","title")+",";
        out+=look_up(path,"/metadata/songs","artist_name")+",";
        out+=look_up(path,"/musicbrainz/songs","year")+",";
        out+=look_up(path,"/analysis/songs","duration")+",";
        out+=look_up(path,"/analysis/songs","tempo");
        System.out.println(out);


       }
       public static String look_up(String Path,String Att,String target){
           String out="";
           try (HdfFile hdfFile=new HdfFile(Paths.get(Path))){
               Dataset dataset = hdfFile.getDatasetByPath(Att);
               Object data =dataset.getData();
               if (data instanceof LinkedHashMap){
                   LinkedHashMap<String,Object>mapdata=(LinkedHashMap<String,Object>)data;
                   for (Map.Entry<String,Object>entry:mapdata.entrySet()){
                       Object v=entry.getValue();
                       //System.out.println(target+","+entry.getKey()+","+(entry.getKey().equals(target)));

                       if (entry.getKey().equals(target)){

                           if(v instanceof int[]){
                               out=String.valueOf(((int[])v)[0]);
                           }
                           else if (v instanceof String[]){
                               out=((String[])v)[0];
                           }
                           else if(v instanceof float[]){
                               out=String.valueOf(((float[])v)[0]);
                           }
                           else  if(v instanceof double[]){
                               out=String.valueOf(((double[])v)[0]);
                           }
                       }
                   }
               }

           }
            return out;
       }

}
