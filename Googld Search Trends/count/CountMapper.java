import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountMapper
    extends Mapper<LongWritable, Text, Text, Text> {

  private static final int MISSING = 9999;
  @Override


  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
        String [] line = (value.toString()).split(",");
        
        String date = line[0];
        String val = "";


        //int[] idx = {22,105,113,140,143,155,224,272,357};
        if (line[0].contains("/")) {
            if (line.length >= 3) {
              val = line[3];
              context.write(new Text(date), new Text(val));
            }
            
            
        } 
      }


      
  }
