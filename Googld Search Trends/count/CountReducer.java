
import java.io.IOException;
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountReducer
    extends Reducer<Text, Text, NullWritable, Text> {
  
  @Override

  public void setup(Context context)  throws IOException, InterruptedException {
    context.write(NullWritable.get(), new Text("Date"+',' + "0 - 9 Years,10 - 19 Years,20 - 29 Years,30 - 39 Years,40 - 49 Years,50 - 59 Years,60 - 69 Years,70 - 79 Years,80+ Years"));
  }
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
        int count1 = 0;
        int count2 = 0;
        int count3 = 0;
        int count4 = 0;
        int count5 = 0;
        int count6 = 0;
        int count7 = 0;
        int count8 = 0;
        int count9 = 0;  
     
        for (Text current : values) { 
          String curr = current.toString();     

          if (curr.contains("10")) {
            count2 ++;           
          }
          else if (curr.contains("20")) {
            count3 ++;           
          }
          else if (curr.contains("30")) {
            count4 ++;           
          }
          else if (curr.contains("40")) {
            count5 ++;    
          }
          else if (curr.contains("50")) {
            count6 ++;           
          }
          else if (curr.contains("60")) {
            count7 ++;           
          }
          else if (curr.contains("70")) {
            count8 ++;           
          }
          else if (curr.contains("80")) {
            count9 ++;            
          } else {
            count1 ++;
          }
           
          
          //context.write(NullWritable.get(), new Text(curr));
   
        }
        String s1 = String.valueOf(count1);
        String s2 = String.valueOf(count2);
        String s3 = String.valueOf(count3);
        String s4 = String.valueOf(count4);
        String s5 = String.valueOf(count5);
        String s6 = String.valueOf(count6);
        String s7 = String.valueOf(count7);
        String s8 = String.valueOf(count8);
        String s9 = String.valueOf(count9);
        context.write(NullWritable.get(), new Text(key.toString()+','+ s1 + ','+ s2 +','+s3+','+s4+','+s5+','+s6+','+s7+','+s8+','+s9));
        
  }
}