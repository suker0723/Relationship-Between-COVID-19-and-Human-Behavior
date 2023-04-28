
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CovidMobilityDataProfileReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int[] count = new int[6];
        int[] sum = new int[6];
        for (Text value : values) {
            String[] fields = value.toString().split("\t");
            for (int i = 0; i < 6; i++) {
                if (!fields[i].equals("NA")) {
                    sum[i] += Integer.parseInt(fields[i]);
                    count[i]++;
                }
            }
        }

        StringBuilder output = new StringBuilder();
        for (int i = 0; i < 6; i++) {
            output.append(count[i] == 0 ? "NA" : (sum[i] / (float) count[i])).append(i == 5 ? "" : "\t");
        }

        result.set(output.toString());
        context.write(key, result);
    }
}
