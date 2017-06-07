//package com.github.ilms49898723.frequentitemsets.PCY;
//
//import com.github.ilms49898723.frequentitemsets.Main;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.NullWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.Writable;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//
//import java.io.*;
//import java.util.ArrayList;
//import java.util.Arrays;
//
//public class PCYSecondPass {
//
//    private static class PCYPair implements Writable {
//        private IntWritable i;
//        private IntWritable j;
//
//        public PCYPair() {
//            i = new IntWritable();
//            j = new IntWritable();
//        }
//
//        public void set(int v1, int v2) {
//            i.set(v1);
//            j.set(v2);
//        }
//
//        public int geti() {
//            return i.get();
//        }
//
//        public int getj() {
//            return j.get();
//        }
//
//        @Override
//        public void write(DataOutput out) throws IOException {
//            i.write(out);
//            j.write(out);
//        }
//
//        @Override
//        public void readFields(DataInput in) throws IOException {
//            i.readFields(in);
//            j.readFields(in);
//        }
//    }
//
//    public static class PCYSecondPassMapper extends Mapper<Object, Text, IntWritable, PCYPair> {
//
//        private static final int HASHTABLE_SIZE = 100007;
//
//        private int[] mItems;
//        private int[] mCandidates;
//        private int mN;
//        private int mK;
//
//        @Override
//        protected void setup(Context context) throws IOException, InterruptedException {
//            super.setup(context);
//
//            Configuration conf = context.getConfiguration();
//            mN = conf.getInt("n", -1);
//            mK = conf.getInt("k", -1);
//            String iPath = conf.get("ItemsetPath");
//            String cPath = conf.get("CandidatePath");
//            mItems = new int[mN];
//            mCandidates = new int[HASHTABLE_SIZE];
//
//            setMap(conf, iPath, cPath);
//        }
//
//        @Override
//        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//
//            String[] tokens = value.toString().split(" ");
//            ArrayList<Integer> items = new ArrayList<>();
//            for (String token : tokens) {
//                items.add(Integer.parseInt(token));
//            }
//            items.sort(Integer::compareTo);
//
//            IntWritable keyOut = new IntWritable(0);
//            for(int i = 0 ; i < items.size() ; i++){
//                if(mItems[i] == 1){
//                    for(int j = i+1 ; j < items.size() ; j++){
//                        int[] idx = {i, j};
//                        if(mItems[j] == 1 && mCandidates[hash(idx, mK)] == 1){
//                            PCYPair valueOut = new PCYPair();
//                            valueOut.set(i, j);
//                            context.write(keyOut, valueOut);
//                        }
//                    }
//                }
//            }
//        }
//
//        private void setMap(Configuration conf, String p1, String p2) throws IOException {
//
//            FileSystem fs = FileSystem.get(conf);
//            Path iPath = new Path(p1);
//            Path cPath = new Path(p2);
//
//            BufferedReader ibr = new BufferedReader(new InputStreamReader(fs.open(iPath)));
//            BufferedReader cbr = new BufferedReader(new InputStreamReader(fs.open(cPath)));
//
//            String line;
//            line = ibr.readLine();
//            while (line != null){
//                String[] tokens = line.split(" ");
//                for(String token : tokens){
//                    mItems[Integer.parseInt(token)] = 1;
//                }
//                line = ibr.readLine();
//            }
//
//            line = cbr.readLine();
//            while (line != null){
//                String[] tokens = line.split(" ");
//                for(String token : tokens){
//                    mCandidates[Integer.parseInt(token)] = 1;
//                }
//                line = cbr.readLine();
//            }
//        }
//
//        private int hash(int[] idx, int k) {
//            if(k == 1){
//                return idx[0] % HASHTABLE_SIZE;
//            } else {
//                int[] nIdx = Arrays.copyOfRange(idx, 0, k-1);
//                return (hash(nIdx, k-1) * 31 + idx[k-1]) % HASHTABLE_SIZE;
//            }
//        }
//    }
//
//    public static class PCYSecondPassReducer extends Reducer<IntWritable, PCYPair, NullWritable, Text> {
//        private static final int HASHTABLE_SIZE = 100007;
//
//        private MultipleOutputs<NullWritable, Text> mMultipleOutputs;
//        private int[] mItems;
//        private int[][] mCount;
//        private int[] mCandidates;
//        private int mN;
//        private int mK;
//        private int mThreshold;
//
//        @Override
//        protected void setup(Context context) throws IOException, InterruptedException {
//            super.setup(context);
//
//            Configuration conf = context.getConfiguration();
//            mN = conf.getInt("n", -1);
//            mK = conf.getInt("k", -1);
//            mThreshold = conf.getInt("threshold", 0);
//            String iPath = conf.get("ItemsetPath");
//            mItems = new int[mN];
//            mCount = new int[mN][mN];
//            mCandidates = new int[HASHTABLE_SIZE];
//            Arrays.fill(mItems, 0);
//            Arrays.fill(mCount, 0);
//            Arrays.fill(mCandidates, 0);
//            setMap(conf, iPath);
//        }
//
//        @Override
//        protected void reduce(IntWritable key, Iterable<PCYPair> values, Context context) throws IOException, InterruptedException {
//            for (PCYPair value : values) {
//                mCount[value.geti()][value.getj()] += 1;
//
//                for(int i = 0 ; i < mN ; i++) {
//                    if(mItems[i] == 1) {
//                        int[] idx = {value.geti(), value.getj(), i};
//                        mCandidates[hash(idx, mK+1)] += 1;
//                    }
//                }
//            }
//        }
//
//        @Override
//        protected void cleanup(Context context) throws IOException, InterruptedException {
//            super.cleanup(context);
//
//            for (int i = 0; i < mN; i++) {
//                for(int j = i+1 ; j < mN ; j++){
//                    if (mCount[i][j] >= mThreshold) {
//                        mMultipleOutputs.write("itemset", NullWritable.get(), new Text(String.valueOf(i) + " " + String.valueOf(j)));
//                    }
//                }
//            }
//
//            for (int i = 0 ; i < HASHTABLE_SIZE ; i++){
//                if(mCandidates[i] >= mThreshold) {
//                    mMultipleOutputs.write("candidate", NullWritable.get(), new Text(String.valueOf(i)));
//                }
//            }
//
//            mMultipleOutputs.close();
//        }
//
//        private void setMap(Configuration conf, String itemPath) {
//
//            FileSystem fs = FileSystem.get(conf);
//            Path iPath = new Path(itemPath);
//
//            BufferedReader ibr = new BufferedReader(new InputStreamReader(fs.open(iPath)));
//
//            String line;
//            line = ibr.readLine();
//            while (line != null){
//                String[] tokens = line.split(" ");
//                for(String token : tokens){
//                    mItems[Integer.parseInt(token)] = 1;
//                }
//                line = br.readLine();
//            }
//        }
//
//        private int hash(int[] values) {
//            int hash = 1;
//            for (int i = 0; i )
//        }
//    }
//
//    public static void run(int k, int n, int threshold, String input) throws IOException, ClassNotFoundException, InterruptedException {
//        Configuration conf = new Configuration();
//        conf.setInt("n", n);
//        conf.setInt("k", k);
//        conf.setInt("threshold", threshold);
//        conf.set("ItemsetPath", "output-" + String.valueof(k-1) + "/itemset");
//        conf.set("CandidatePath", "output-" + String.valueof(k-1) + "/candidate");
//        Job job = new Job(conf, "Frequent Itemsets Pass " + String.valueof(k));
//
//        job.setJarByClass(Main.class);
//        job.setMapOutputKeyClass(IntWritable.class);
//        job.setMapOutputValueClass(PCYFirstValue.class);
//        job.setOutputKeyClass(NullWritable.class);
//        job.setOutputValueClass(Text.class);
//        job.setMapperClass(PCYFirstMapper.class);
//        job.setReducerClass(PCYFirstReducer.class);
//        job.setInputFormatClass(TextInputFormat.class);
//        job.setOutputFormatClass(TextOutputFormat.class);
//        job.setNumReduceTasks(1);
//
//        FileInputFormat.addInputPath(job, new Path(input));
//        FileOutputFormat.setOutputPath(job, new Path("output-" + String.valueof(k)));
//        MultipleOutputs.addNamedOutput(job, "itemset", TextOutputFormat.class, NullWritable.class, Text.class);
//        MultipleOutputs.addNamedOutput(job, "candidate", TextOutputFormat.class, NullWritable.class, Text.class);
//
//        job.waitForCompletion(true);
//    }
//}
>>>>>>> 55ca02412e72efa6187226f02d2abb8c8d6860ee
