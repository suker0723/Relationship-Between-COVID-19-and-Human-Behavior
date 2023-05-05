import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TwitterCountMapper
    extends Mapper<LongWritable, Text, Text, IntWritable> {
  private static final int MISSING = 9999;
  private final static IntWritable one = new IntWritable(1);

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    String line = value.toString().toLowerCase();
    String[] tokens = line.split(",");
    if (tokens.length < 3) {
      return;
    }
    String token = tokens[2];
    if (token.length() == 10) { // This is to check the date format
      context.write(new Text(token), one);
    }
  }
}
