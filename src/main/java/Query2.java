import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;

public class Query2 {

    public static class CountryCodeMapper
            extends Mapper<LongWritable, Text, IntWritable, Text>{

        private final IntWritable contextKey = new IntWritable(); // Should be country code
        private final Text contextValue = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");

            // customerID, name, age, gender, countryCode, salary
            // Needs to be map-reduce job because it requires aggregation and summing
            int customerID = Integer.parseInt(fields[0]);
            int countryCode = Integer.parseInt(fields[4]);

            contextKey.set(customerID);
            contextValue.set("C," + countryCode);
            context.write(contextKey, contextValue);
        }
    }

    public static class TransactionMapper
            extends Mapper<LongWritable, Text, IntWritable, Text>{

        private final IntWritable contextKey = new IntWritable();
        private final Text contextValue = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");

            // transactionID, customerID, transTotal, transNumItems, transDesc
            int customerID = Integer.parseInt(fields[1]);
            float transTotal = Float.parseFloat(fields[2]);

            contextKey.set(customerID);
            contextValue.set("T," + transTotal);

            context.write(contextKey, contextValue);
        }
    }

    public static class Query2Reducer
            extends Reducer<IntWritable, Text, IntWritable, Text> {

        public static class CountryStats {
            int numCustomers;
            float minTransTotal;
            float maxTransTotal;

            public CountryStats() {
                numCustomers = 0;
                minTransTotal = Float.MAX_VALUE;
                maxTransTotal = 0f;
            }
        }

        // Hash map allows us to store country stats without regrouping on countrycode, 1 map-reduce job
        private final HashMap<Integer, CountryStats> countryMap = new HashMap<>();

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int countryCode = -1;
            float curMin = Float.MAX_VALUE;
            float curMax = 0;

            for (Text val : values) {
                String[] fields = val.toString().split(",");

                if (fields[0].equals("C")) {
                    countryCode = Integer.parseInt(fields[1]);
                } else if (fields[0].equals("T")) {
                    float transTotal = Float.parseFloat(fields[1]);
                    curMin = Math.min(curMin, transTotal);
                    curMax = Math.max(curMax, transTotal);
                }
            }
            CountryStats countryStats = countryMap.computeIfAbsent(countryCode, k -> new CountryStats());

            countryStats.numCustomers++;
            countryStats.minTransTotal = Math.min(countryStats.minTransTotal, curMin);
            countryStats.maxTransTotal = Math.max(countryStats.maxTransTotal, curMax);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            IntWritable countryCode = new IntWritable();
            Text outputValue = new Text();

            for (HashMap.Entry<Integer, CountryStats> entry : countryMap.entrySet()) {

                int code = entry.getKey();
                CountryStats stats = entry.getValue();

                String output = stats.numCustomers + "," + stats.minTransTotal + "," + stats.maxTransTotal;

                countryCode.set(code);
                outputValue.set(output);

                context.write(countryCode, outputValue);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Query2");
        job.setJarByClass(Query2.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(Query2.Query2Reducer.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Query2.CountryCodeMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, Query2.TransactionMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
