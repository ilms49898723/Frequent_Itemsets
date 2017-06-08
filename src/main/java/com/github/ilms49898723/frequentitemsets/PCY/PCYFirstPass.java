package com.github.ilms49898723.frequentitemsets.PCY;

import com.github.ilms49898723.frequentitemsets.Main;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class PCYFirstPass {
    private static class PCYFirstValue implements Writable {
        private ArrayList<Integer> mItems;

        public PCYFirstValue() {
            mItems = new ArrayList<>();
        }

        public ArrayList<Integer> getItems() {
            return mItems;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(mItems.size());
            for (int value : mItems) {
                out.writeInt(value);
            }
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            int size = in.readInt();
            mItems = new ArrayList<>();
            for (int i = 0; i < size; ++i) {
                mItems.add(in.readInt());
            }
        }
    }

    public static class PCYFirstMapper extends Mapper<Object, Text, IntWritable, PCYFirstValue> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(" ");
            PCYFirstValue outValue = new PCYFirstValue();
            for (String token : tokens) {
                outValue.getItems().add(Integer.parseInt(token));
            }
            outValue.getItems().sort(Integer::compareTo);
            IntWritable outKey = new IntWritable(outValue.getItems().get(0));
            context.write(outKey, outValue);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

    public static class PCYFirstReducer extends Reducer<IntWritable, PCYFirstValue, NullWritable, Text> {
        private static final int HASHTABLE_SIZE = 100007;

        private MultipleOutputs<NullWritable, Text> mMultipleOutputs;
        private int[] mCount;
        private int[] mHashTables;
        private int mN;
        private int mThreshold;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            mMultipleOutputs = new MultipleOutputs<>(context);
            mN = conf.getInt("n", -1);
            mThreshold = conf.getInt("threshold", Integer.MAX_VALUE);
            mCount = new int[mN];
            mHashTables = new int[HASHTABLE_SIZE];
            Arrays.fill(mCount, 0);
            Arrays.fill(mHashTables, 0);
        }

        @Override
        protected void reduce(IntWritable key, Iterable<PCYFirstValue> values, Context context) throws IOException, InterruptedException {
            for (PCYFirstValue value : values) {
                ArrayList<Integer> items = value.getItems();
                for (int item : items) {
                    mCount[item] += 1;
                }
                for (int i = 0; i < items.size(); ++i) {
                    for (int j = i + 1; j < items.size(); ++j) {
                        int candidates[] = new int[] { items.get(i), items.get(j) };
                        mHashTables[PCYUtil.hash(candidates, HASHTABLE_SIZE)] += 1;
                    }
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            for (int i = 0; i < mN; ++i) {
                if (mCount[i] >= mThreshold) {
                    mMultipleOutputs.write("itemsets", NullWritable.get(), new Text(String.valueOf(i)));
                }
            }
            for (int i = 0; i < HASHTABLE_SIZE; ++i) {
                if (mHashTables[i] >= mThreshold) {
                    mMultipleOutputs.write("candidates", NullWritable.get(), new Text(String.valueOf(i)));
                }
            }
            mMultipleOutputs.close();
        }
    }

    public static void run(Configuration conf, int k, int n, int threshold, String input) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration(conf);
        Configuration jConf = new Configuration(configuration);
        jConf.setInt("n", n);
        jConf.setInt("k", k);
        jConf.setInt("threshold", threshold);
        Job job = Job.getInstance(jConf, "Frequent Itemsets Pass 1");
        job.setJarByClass(Main.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(PCYFirstValue.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(PCYFirstMapper.class);
        job.setReducerClass(PCYFirstReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path("frequent-itemsets-1"));
        MultipleOutputs.addNamedOutput(job, "candidates", TextOutputFormat.class, NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "itemsets", TextOutputFormat.class, NullWritable.class, Text.class);
        job.waitForCompletion(true);
    }
}
