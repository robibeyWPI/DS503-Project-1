import java.io.IOException;

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

public class Query1 {

    public static class CustomerMapper
            extends Mapper<LongWritable, Text, IntWritable, Text>{

        private final IntWritable contextKey = new IntWritable();
        private final Text contextValue = new Text();

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");

            // customerID, name, age, gender, countryCode, salary
            int customerID = Integer.parseInt(fields[0]);
            String customerName = fields[1];
            float customerSalary = Float.parseFloat(fields[5]);

            contextKey.set(customerID);
            contextValue.set("C," + customerName + "," + customerSalary);
            context.write(contextKey, contextValue);
            }
        }
    public static class TransactionMapper
            extends Mapper<LongWritable, Text, IntWritable, Text> {

        private final IntWritable contextKey = new IntWritable();
        private final Text contextValue = new Text();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] fields = value.toString().split(",");

            // transactionID, customerID, transTotal, transNumItems, transDesc
            int customerID = Integer.parseInt(fields[1]);
            int numItems = Integer.parseInt(fields[3]);
            float transTotal = Float.parseFloat(fields[2]);

            contextKey.set(customerID);
            contextValue.set("T," + numItems + "," + transTotal);

            context.write(contextKey, contextValue);
        }
    }

    public static class ReportReducer
            extends Reducer<IntWritable,Text,IntWritable,Text> {
        private final Text result = new Text();

        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            String Name = "";
            float Salary = 0f;
            int NumOfTransactions = 0;
            float TotalSum = 0f;
            int MinItems = Integer.MAX_VALUE;

            for (Text val : values) {
                String[] fields = val.toString().split(",");

                if (fields[0].equals("C")) {
                    Name = fields[1];
                    Salary = Float.parseFloat(fields[2]);
                } else if (fields[0].equals("T")) {
                    int items = Integer.parseInt(fields[1]);
                    float transTotal = Float.parseFloat(fields[2]);

                    NumOfTransactions++;
                    TotalSum += transTotal;
                    MinItems = Math.min(MinItems, items);
                }
            }
            if (NumOfTransactions == 0) {
                MinItems = 0;
            }
            result.set(Name + "," + Salary + "," + NumOfTransactions + "," + TotalSum + "," + MinItems);
            context.write(key, result);
        }
    }

    public void debug(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Customer Transaction Report");
        job.setJarByClass(Query1.class);
        job.setReducerClass(ReportReducer.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CustomerMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TransactionMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Query1");
        job.setJarByClass(Query1.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(ReportReducer.class);
        job.setNumReduceTasks(1);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, CustomerMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, TransactionMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}