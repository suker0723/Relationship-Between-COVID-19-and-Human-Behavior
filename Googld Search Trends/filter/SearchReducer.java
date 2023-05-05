
import java.io.IOException;
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SearchReducer
    extends Reducer<Text, Text, NullWritable, Text> {
  
  @Override

  public void setup(Context context)  throws IOException, InterruptedException {
    context.write(NullWritable.get(), new Text("Date"+',' + "location_key,anxiety,depression,dry_eye_syndromes, fever, gastroparesis, fatigue, insomnia, neck_pain,skin condition"));
  }
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
 
       
        
        for (Text current : values) {
          
            context.write(NullWritable.get(), new Text(key.toString()+','+ current.toString()));

        }
        
  }
}