import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SearchMapper1
    extends Mapper<LongWritable, Text, Text, Text> {

  private static final int MISSING = 9999;
  @Override


  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
        String [] line = (value.toString()).split(",");
        
        String date = line[0];
        date = date.replace("-", "/");
        String vals = ""; 
        boolean valid = true;
        int[] idx = {22,105,113,140,143,155,224,272,357};
        if (line[0].contains("-") && line[1].contains("US")) {
          
            for (int i = 0; i < 9; i++) {

              if (idx[i] < line.length) {
                if (line[idx[i]].length() < 1) {
                  valid = false;
                  break;
                } else {
                  vals += line[idx[i]] + ",";
                }
              
              }
              
            }
          if (valid && vals.length() > 0) {
            context.write(new Text(date), new Text(vals.substring(0, vals.length()-1)));
          }
          
        } 


      
  }
}