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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class PCYSecondPass {
	
    private static class PCYSecondValue implements Writable {
		private String type;
        private ArrayList<Integer> mItems;

        public PCYSecondValue() {
            mItems = new ArrayList<>();
			type = new String();
        }

        public ArrayList<Integer> getItems() {
            return mItems;
        }
		
		public void setType(String t) {
			type = new String(t);
		}
		
		public String getType() {
			return type;
		}

        @Override
        public void write(DataOutput out) throws IOException {
			out.writeChars(type);
            out.writeInt(mItems.size());
            for (int value : mItems) {
                out.writeInt(value);
            }
        }

        @Override
        public void readFields(DataInput in) throws IOException {
			type = new String(in.readLine());
            int size = in.readInt();
            mItems = new ArrayList<>();
            for (int i = 0; i < size; ++i) {
                mItems.add(in.readInt());
            }
        }
    }
	
	private static class Pair {
		public int i;
		public int j;
		
		public Pair(int i, int j) {
			this.i = i;
			this.j = j;
		}
	}

    public static class PCYSecondItemSetMapper extends Mapper<Object, Text, IntWritable, PCYSecondValue> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(" ");
            PCYSecondValue outValue = new PCYSecondValue();
            for (String token : tokens) {
                outValue.getItems().add(Integer.parseInt(token));
            }
            outValue.getItems().sort(Integer::compareTo);
			outValue.setType("I");
            IntWritable outKey = new IntWritable(outValue.getItems().get(0));
            context.write(outKey, outValue);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }
	
	public static class PCYSecondCandidateMapper extends Mapper<Object, Text, IntWritable, PCYSecondValue> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(" ");
            PCYSecondValue outValue = new PCYSecondValue();
            for (String token : tokens) {
                outValue.getItems().add(Integer.parseInt(token));
            }
            outValue.getItems().sort(Integer::compareTo);
			outValue.setType("C");
            IntWritable outKey = new IntWritable(outValue.getItems().get(0));
            context.write(outKey, outValue);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

    public static class PCYFirstReducer extends Reducer<IntWritable, PCYSecondValue, NullWritable, Text> {
        private static final int HASHTABLE_SIZE = 100007;

        private MultipleOutputs<NullWritable, Text> mMultipleOutputs;
		private int[] mItems;
		private int[] mCandidates;
        private ArrayList<Pair> mPairs;
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
			mItems = new int[mN];
			mCandidates = new int [HASHTABLE_SIZE];
			mPairs = new ArrayList<Pair>();
			Array.fill(mItems, 0);
			Array.fill(mCandidates, 0);
            Arrays.fill(mHashTables, 0);
        }

        @Override
        protected void reduce(IntWritable key, Iterable<PCYSecondValue> values, Context context) throws IOException, InterruptedException {
            for (PCYSecondValue value : values) {
                ArrayList<Integer> items = value.getItems();
				
				switch(value.getType()){
					case "I":{
						for(int item : items) {
							mItems[item] = 1;
						}
						break;
					}
					case "C":{
						for(int item : items) {
							mCandidates[item] = 1;
						}
						break;
					}
					default:{
						System.err.println("- Error: Type of SecondPass Value is wrong: " + value.getType);
						break;
					}
				}
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
			
            for (int i = 0; i < mN; i++) {
				for(int j = 0 ; j < mN ; j++){
					if (mItems[i] == 1 && mItems[j] == 1 && mCandidates[hash(i, j)] == 1) {
						mPairs.add(new Pair(i, j));
						mMultipleOutputs.write("count", NullWritable.get(), new Text(String.valueOf(i) + "," + String.valueOf(j)));
					}
				}
            }
			
            mMultipleOutputs.close();
        }

        private int hash(int a, int b) {
            return (a * 31 + b) % HASHTABLE_SIZE;
        }
    }

    public static void run(int k, int n, int threshold) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.setInt("n", n);
        conf.setInt("k", k);
        conf.setInt("threshold", threshold);
        Job job = new Job(conf, "Frequent Itemsets Pass 2");
		
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
		
		MultipleInputs.addInputPath(job, "output-1/itemset", TextInputFormat.class, PCYSecondItemSetMapper.class);
		MultipleInputs.addInputPath(job, "output-1/candidate", TextInputFormat.class, PCYSecondCandidateMapper.class);
        FileOutputFormat.setOutputPath(job, new Path("output-2"));
        MultipleOutputs.addNamedOutput(job, "count", TextOutputFormat.class, NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "candidate", TextOutputFormat.class, NullWritable.class, Text.class);
		
        job.waitForCompletion(true);
    }
}
