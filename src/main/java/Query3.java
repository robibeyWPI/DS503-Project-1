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
import java.util.Map;

public class Query3 {

    public static class CustomerMapper
            extends Mapper<LongWritable, Text, IntWritable, Text>{

        private final IntWritable contextKey = new IntWritable();
        private final Text contextValue = new Text();

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");

            // customerID, name, age, gender, countryCode, salary
            int customerID = Integer.parseInt(fields[0]);
            int customerAge = Integer.parseInt(fields[2]);
            String customerGender = fields[3];

            contextKey.set(customerID);
            contextValue.set("C," + customerAge + "," + customerGender);
            context.write(contextKey, contextValue);
        }
    }

    public static class TransactionMapper
            extends Mapper<LongWritable, Text, IntWritable, Text>{

        private final IntWritable contextKey = new IntWritable();
        private final Text contextValue = new Text();

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");

            // transactionID, customerID, transTotal, transNumItems, transDesc
            int customerID = Integer.parseInt(fields[1]);
            float transTotal = Float.parseFloat(fields[2]);

            contextKey.set(customerID);
            contextValue.set("T," + transTotal);
            context.write(contextKey, contextValue);
        }
    }

    public static class Query3Reducer
            extends Reducer<IntWritable, Text, Text, Text> {

        public static class AgeGenderStats {
            // Need number of transactions for avg, as well as a sum
            int numTransactions;
            float sumTransTotal;
            float minTransTotal;
            float maxTransTotal;

            public AgeGenderStats() {
                numTransactions = 0;
                sumTransTotal = 0f;
                minTransTotal = Float.MAX_VALUE;
                maxTransTotal = 0f;
            }
        }

        private String getAgeRange(int age) {
            if (age >= 10 && age < 20) return "[10,20)";
            else if (age >= 20 && age < 30) return "[20,30)";
            else if (age >= 30 && age < 40) return "[30,40)";
            else if (age >= 40 && age < 50) return "[40,50)";
            else if (age >= 50 && age < 60) return "[50,60)";
            else if (age >= 60 && age <= 70) return "[60,70]";
            else return "Age not defined";
        }


        private final HashMap<String, AgeGenderStats> ageGenderMap = new HashMap<>();

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            float curSum = 0f;
            float curMin = Float.MAX_VALUE;
            float curMax = 0f;
            int curCount = 0;
            String curAgeRange = "";
            String curGender = "";


            for (Text val : values) {
                String[] fields = val.toString().split(",");

                if (fields[0].equals("C")) {
                    int curAge = Integer.parseInt(fields[1]);
                    curAgeRange = getAgeRange(curAge);
                    curGender = fields[2];
                } else if (fields[0].equals("T")) {
                    float curTransTotal = Float.parseFloat(fields[1]);

                    curMin = Math.min(curMin, curTransTotal);
                    curMax = Math.max(curMax, curTransTotal);
                    curSum += curTransTotal;
                    curCount++;
                }
            }
            String mapKey = curAgeRange + "," + curGender;
            AgeGenderStats stats = ageGenderMap.computeIfAbsent(mapKey, k -> new AgeGenderStats());

            stats.numTransactions += curCount;
            stats.sumTransTotal += curSum;
            stats.minTransTotal = Math.min(stats.minTransTotal, curMin);
            stats.maxTransTotal = Math.max(stats.maxTransTotal, curMax);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Text outputKey = new Text();
            Text outputValue = new Text();

            for (Map.Entry<String, AgeGenderStats> entry : ageGenderMap.entrySet()) {
                String ageGenderKey = entry.getKey();
                AgeGenderStats stats = entry.getValue();
                float avgTransTotal = stats.sumTransTotal /  stats.numTransactions;

                String output = stats.minTransTotal + "," + stats.maxTransTotal + "," + avgTransTotal;

                outputKey.set(ageGenderKey);
                outputValue.set(output);
                context.write(outputKey, outputValue);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Query3");
        job.setJarByClass(Query3.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(Query3.Query3Reducer.class);
        job.setNumReduceTasks(1);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Query3.CustomerMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, Query3.TransactionMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
