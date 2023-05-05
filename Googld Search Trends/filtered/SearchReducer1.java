
import java.io.IOException;
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SearchReducer1
    extends Reducer<Text, Text, NullWritable, Text> {
  
  @Override

  public void setup(Context context)  throws IOException, InterruptedException {
    context.write(NullWritable.get(), new Text("Date"+',' + "anxiety,depression,dry_eye_syndromes, fever, gastroparesis, fatigue, insomnia, neck_pain,skin condition"));
  }
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
        int count = 0;
        Double[] sumof = {0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0};
        for (Text current : values) {
            String[] curr = current.toString().split(",");
            int i = 0;

            for (String s: curr) {
              if (!s.isEmpty()) {
                sumof[i] += Double.parseDouble(s);
                i++;
              }

            }
            count += 1;
        }
        String val = "";
        for (int i = 0; i < 9; i++) {
          Double v = sumof[i];
          v /= count;
          val += "," + String.valueOf(v);

        }
        context.write(NullWritable.get(),new Text(key.toString() + val));
        
  }
}