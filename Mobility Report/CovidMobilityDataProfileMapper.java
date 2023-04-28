
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CovidMobilityDataProfileMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text monthKey = new Text();
    private Text values = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String input = value.toString();
        if (input.charAt(input.length() - 1) == ',') {
            input += "NA";
        }

        String[] fields = input.split(",");

        if (fields.length == 15 && Objects.equals(fields[0], "US")) {
            String[] dateParts = fields[8].split("-");
            if(dateParts.length < 2){
                return;
            }
            String year = dateParts[0];
            String month = dateParts[1];
            String day = dateParts[2];
            String outputKey = year + "/" + month +"/"+ day;

            StringBuilder outputValue = new StringBuilder();
            for (int i = 9; i <= 14; i++) {
                outputValue.append(fields[i].isEmpty() ? "NA" : fields[i]).append(i == 14 ? "" : "\t");
            }

            monthKey.set(outputKey);
            values.set(outputValue.toString());
            context.write(monthKey, values);
        }
    }
}
