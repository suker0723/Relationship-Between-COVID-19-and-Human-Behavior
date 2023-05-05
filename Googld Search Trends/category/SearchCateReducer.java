
import java.io.IOException;
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SearchCateReducer
    extends Reducer<Text, Text, NullWritable, Text> {
  
  @Override

  public void setup(Context context)  throws IOException, InterruptedException {
    context.write(NullWritable.get(), new Text("Date"+',' + "Mental" +','+ "Physical"));
  }
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
 
        Double mental = 0.0;
      //String[] symptons = {"anxiety", "depression", "dry eye syndromes", "fever", "gastroparesis", "fatigue", "insomnia", "neck pain","skin condition"};
        Double physcial = 0.0;
   
        for (Text current : values) {
          
            String[] temp = current.toString().split("\\s+");
            
              String[] curr = {"","","","","","","","",""};
              int i = 0;
              for (String s:temp) {
                if (!s.isEmpty()) {
                  curr[i] = s;
                  i ++;
                }
              }
              if (i == 9 ) {
                mental += Double.parseDouble(curr[0]);
                mental += Double.parseDouble(curr[1]);
                mental += Double.parseDouble(curr[5]);
                mental += Double.parseDouble(curr[6]);
                physcial += Double.parseDouble(curr[2]);
                physcial += Double.parseDouble(curr[3]);
                physcial += Double.parseDouble(curr[4]);
                physcial += Double.parseDouble(curr[7]);
                physcial += Double.parseDouble(curr[8]);
                context.write(NullWritable.get(), new Text(key.toString()+','+ String.valueOf(mental/4) + "," + String.valueOf(physcial/5)));
              }
  
              
              
              
            

            
        }
        
        
  }
}