import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Scanner;
import java.util.StringTokenizer;

public class TotalVaccines {
    public static class TotalVaccinesMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Scanner scanner = new Scanner(value.toString());
            String row = "";

            if (scanner.hasNextLine()) {
                row = scanner.nextLine();
            }

            String[] data = row.split(",");
            String date = "";
            String remain = "";

            for (int i = 0; i < data.length; i++) {
                if (i == 0) {
                    date = data[i];
                }
                if (date.compareTo("2021-02-22") >= 0 && date.compareTo("2021-12-30") <= 0) {
                    if (i == 3) {
                        remain += data[i];
                    }
                    if (i > 3) {
                        remain += "," + data[i];
                    }
                }
            }

            context.write(new Text(), new Text(remain));
        }
    }

    public static class TotalVaccinesReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int[] doses = new int[12];
            int sum = 0;
            for (Text val : values) {
                StringTokenizer itr = new StringTokenizer(val.toString(), ",");
                for (int i = 0; itr.hasMoreTokens(); i++) {
                    doses[i] += Integer.parseInt(itr.nextToken().trim());
                }
            }
            for (int dose : doses) {
                sum += dose;
            }

            context.write(new Text("Total number of vaccinations: " + sum), new Text());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length < 2) {
            System.err.println("Usage: TotalVaccines <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "TotalVaccines by Jinchuan");
        job.setJarByClass(TotalVaccines.class);

        job.setMapperClass(TotalVaccinesMapper.class);
        job.setReducerClass(TotalVaccinesReducer.class);
        job.setOutputKeyClass(Text.class); //output key
        job.setOutputValueClass(Text.class); //output value

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
