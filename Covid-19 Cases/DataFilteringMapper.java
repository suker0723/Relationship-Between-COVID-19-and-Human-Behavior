import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DataFilteringMapper
    extends Mapper<LongWritable, Text, NullWritable, Text> {
  
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
    String line = value.toString();
    String[] features = line.split(",");
    
    // Columns needed to be kept
    String earliestDt = features[0];
    String sex = features[5];
    String age = features[6];
    // 
    String race = features[7];
    if (features.length == 13) {
      race = race + "; "+features[8];
    }
    String icu_yn = features[features.length - 3];
    String death_yn = features[features.length - 2];
    String medcon_yn = features[features.length - 1];

    String newLine = earliestDt + "," + sex + "," + age + "," +
                             race + "," + icu_yn + "," + death_yn + "," + medcon_yn;
    context.write(NullWritable.get(), new Text(newLine));
  }
}
