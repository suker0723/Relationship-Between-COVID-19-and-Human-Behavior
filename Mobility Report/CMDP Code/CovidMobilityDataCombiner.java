

import java.io.IOException;
        import java.util.Iterator;
        import org.apache.hadoop.io.Text;
        import org.apache.hadoop.mapreduce.Reducer;

public class CovidMobilityDataCombiner extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int[] count = new int[6];
        int[] sum = new int[6];
        Iterator<Text> iterator = values.iterator();
        while (iterator.hasNext()) {
            String[] fields = iterator.next().toString().split("\t");
            for (int i = 0; i < 6; i++) {
                String[] sumCount = fields[i].split(",");
                sum[i] += Integer.parseInt(sumCount[0]);
                count[i] += Integer.parseInt(sumCount[1]);
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

