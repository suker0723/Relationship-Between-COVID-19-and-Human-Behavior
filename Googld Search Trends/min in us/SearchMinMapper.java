import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SearchMinMapper
    extends Mapper<LongWritable, Text, Text, Text> {

  private static final int MISSING = 9999;
  @Override


  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
        String [] line = (value.toString()).split(",");
        
        String date = line[0];
        String loc = line[1];
        String vals = ""; 
        //int[] idx = {22,105,113,140,143,155,224,272,357};
        if (line[0].contains("-")) {
          if (line[1].contains("US")) {
            for (int i = 2; i < line.length; i++) {
              vals += line[i] + " ";
            }
            
            context.write(new Text(date+','+loc), new Text(vals.trim()));
        } 
      }


      
  }
}