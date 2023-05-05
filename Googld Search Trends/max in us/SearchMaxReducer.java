
import java.io.IOException;
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SearchMaxReducer
    extends Reducer<Text, Text, NullWritable, Text> {
  
  @Override

  public void setup(Context context)  throws IOException, InterruptedException {
    context.write(NullWritable.get(), new Text("Date,loc"+',' + "Sympton" +','+ "Search_Volume"));
  }
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
 
        Double max_now = 0.0;
        String[] symptons = {"anxiety", "depression", "dry eye syndromes", "fever", "gastroparesis", "fatigue", "insomnia", "neck pain","skin condition"};
        int idx = 0;
        int i = 0;
        for (Text current : values) {
          
            String[] curr = current.toString().split("\\s+");
            
              for (String s: curr) {
                
                if (!s.isEmpty()) {
                  if (Double.parseDouble(s) >= max_now) {
                    max_now = Double.parseDouble(s);
                    idx = i;
                  }
                  i++;                
                }   
              
                
    
            }
            context.write(NullWritable.get(), new Text(key.toString()+','+ symptons[idx] + "," + String.valueOf(max_now)));

            





        }
        
  }
}