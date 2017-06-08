package com.github.ilms49898723.frequentitemsets.PCY;

import com.github.ilms49898723.frequentitemsets.Main;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

public class PCYSolver {
    private static class PCYKey implements WritableComparable<PCYKey> {
        private ArrayList<Integer> mKeys;

        public PCYKey() {
            mKeys = new ArrayList<>();
        }

        public ArrayList<Integer> getKeys() {
            return mKeys;
        }

        @Override
        public int compareTo(PCYKey o) {
            if (mKeys.size() != o.mKeys.size()) {
                return Integer.compare(mKeys.size(), o.mKeys.size());
            } else {
                for (int i = 0; i < mKeys.size(); ++i) {
                    if (!mKeys.get(i).equals(o.mKeys.get(i))) {
                        return Integer.compare(mKeys.get(i), o.mKeys.get(i));
                    }
                }
                return 0;
            }
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(mKeys.size());
            for (int key : mKeys) {
                out.writeInt(key);
            }
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            mKeys = new ArrayList<>();
            int size = in.readInt();
            for (int i = 0; i < size; ++i) {
                mKeys.add(in.readInt());
            }
        }
    }

    private static class PCYValue implements Writable {
        private ArrayList<Integer> mValues;

        public PCYValue() {
            mValues = new ArrayList<>();
        }

        public ArrayList<Integer> getValues() {
            return mValues;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(mValues.size());
            for (int value: mValues) {
                out.writeInt(value);
            }
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            mValues = new ArrayList<>();
            int size = in.readInt();
            for (int i = 0; i < size; ++i) {
                mValues.add(in.readInt());
            }
        }
    }

    private static class SolverMapper extends Mapper<Object, Text, PCYKey, PCYValue> {
        private static final int HASHTABLE_SIZE = 10007;

        private HashSet<Integer> mHashes;
        private HashSet<Integer> mFrequentItemsKMinus1;
        private HashSet<Integer> mFrequentItems;
        private ArrayList<Integer> mElements;
        private Context mContext;
        private int mK;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            mK = context.getConfiguration().getInt("k", -1);
            mHashes = new HashSet<>();
            mFrequentItemsKMinus1 = new HashSet<>();
            mFrequentItems = new HashSet<>();
            initializeHashes(context);
            initializeFrequentItems(context);
            initializeFrequentItemsKMinus1(context);
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(" ");
            mContext = context;
            mElements = new ArrayList<>();
            for (String token : tokens) {
                mElements.add(Integer.parseInt(token));
            }
            mElements.sort(Integer::compareTo);
            generateSets(0, 0, new ArrayList<>());
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }

        private void initializeHashes(Context context) {
            try {
                int k = context.getConfiguration().getInt("k", -1) - 1;
                FileSystem fileSystem = FileSystem.get(context.getConfiguration());
                FileStatus[] fileStatuses = fileSystem.listStatus(new Path("frequent-itemsets-" + k));
                for (FileStatus fileStatus : fileStatuses) {
                    String[] fileName = fileStatus.getPath().toString().split("/");
                    if (fileName[fileName.length - 1].contains("candidates")) {
                        BufferedReader reader = new BufferedReader(
                                new InputStreamReader(fileSystem.open(fileStatus.getPath()))
                        );
                        String line;
                        while ((line = reader.readLine()) != null) {
                            mHashes.add(Integer.parseInt(line));
                        }
                        reader.close();
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private void initializeFrequentItems(Context context) {
            try {
                FileSystem fileSystem = FileSystem.get(context.getConfiguration());
                FileStatus[] fileStatuses = fileSystem.listStatus(new Path("frequent-itemsets-1"));
                for (FileStatus fileStatus : fileStatuses) {
                    String[] fileName = fileStatus.getPath().toString().split("/");
                    if (fileName[fileName.length - 1].contains("itemsets")) {
                        BufferedReader reader = new BufferedReader(
                                new InputStreamReader(fileSystem.open(fileStatus.getPath()))
                        );
                        String line;
                        while ((line = reader.readLine()) != null) {
                            mFrequentItems.add(Integer.parseInt(line));
                        }
                        reader.close();
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private void initializeFrequentItemsKMinus1(Context context) {
            try {
                FileSystem fileSystem = FileSystem.get(context.getConfiguration());
                FileStatus[] fileStatuses = fileSystem.listStatus(new Path("frequent-itemsets-" + (mK - 1)));
                for (FileStatus fileStatus : fileStatuses) {
                    String[] fileName = fileStatus.getPath().toString().split("/");
                    if (fileName[fileName.length - 1].contains("itemsets")) {
                        BufferedReader reader = new BufferedReader(
                                new InputStreamReader(fileSystem.open(fileStatus.getPath()))
                        );
                        String line;
                        while ((line = reader.readLine()) != null) {
                            String[] tokens = line.split(" ");
                            ArrayList<Integer> set = new ArrayList<>();
                            for (String token : tokens) {
                                set.add(Integer.parseInt(token));
                            }
                            set.sort(Integer::compareTo);
                            mFrequentItemsKMinus1.add(PCYUtil.hash(set, HASHTABLE_SIZE));
                        }
                        reader.close();
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private void generateSets(int now, int index, ArrayList<Integer> elements) throws IOException, InterruptedException {
            if (now == mK) {
                PCYKey newKey = new PCYKey();
                int hashValue = PCYUtil.hash(elements, HASHTABLE_SIZE);
                if (mHashes.contains(hashValue)) {
                    PCYValue newValue = new PCYValue();
                    newKey.getKeys().addAll(elements);
                    if (checkNewSet(newKey.getKeys())) {
                        for (int value : mElements) {
                            if (!newKey.getKeys().contains(value) && mFrequentItems.contains(value)) {
                                newValue.getValues().add(value);
                            }
                        }
                        mContext.write(newKey, newValue);
                    }
                }
                return;
            }
            for (int i = index; i < mElements.size(); ++i) {
                if (mFrequentItems.contains(elements.get(i))) {
                    elements.add(mElements.get(i));
                    generateSets(now + 1, i + 1, elements);
                    elements.remove(elements.size() - 1);
                }
            }
        }

        private boolean checkNewSet(ArrayList<Integer> newSet) {
            for (int i = 0; i < newSet.size(); ++i) {
                ArrayList<Integer> subset = new ArrayList<>();
                subset.addAll(newSet.subList(0, i));
                if (i + 1 < newSet.size()) {
                    subset.addAll(newSet.subList(i + 1, newSet.size()));
                }
                if (!mFrequentItemsKMinus1.contains(PCYUtil.hash(subset, HASHTABLE_SIZE))) {
                    return false;
                }
            }
            return true;
        }
    }

    private static class SolverReducer extends Reducer<PCYKey, PCYValue, NullWritable, Text> {
        private static final int HASHTABLE_SIZE = 10007;

        private MultipleOutputs<NullWritable, Text> mMultipleOutputs;
        private int[] mHashTable;
        private int mThreshold;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            mMultipleOutputs = new MultipleOutputs<>(context);
            mHashTable = new int[HASHTABLE_SIZE];
            mThreshold = conf.getInt("threshold", Integer.MAX_VALUE);
            Arrays.fill(mHashTable, 0);
        }

        @Override
        protected void reduce(PCYKey key, Iterable<PCYValue> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            int[] localHashTable = new int[HASHTABLE_SIZE];
            Arrays.fill(localHashTable, 0);
            for (PCYValue value : values) {
                for (int element : value.getValues()) {
                    ArrayList<Integer> candidate = new ArrayList<>();
                    candidate.addAll(key.getKeys());
                    candidate.add(element);
                    candidate.sort(Integer::compareTo);
                    localHashTable[PCYUtil.hash(candidate, HASHTABLE_SIZE)] += 1;
                }
                ++count;
            }
            if (count >= mThreshold) {
                String output = "";
                boolean isFirst = true;
                for (int element : key.getKeys()) {
                    output += String.valueOf(element);
                    if (!isFirst) {
                        output += " ";
                    } else {
                        isFirst = false;
                    }
                }
                mMultipleOutputs.write("itemsets", NullWritable.get(), new Text(output));
                for (int i = 0; i < HASHTABLE_SIZE; ++i) {
                    mHashTable[i] += localHashTable[i];
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            for (int i = 0; i < HASHTABLE_SIZE; ++i) {
                if (mHashTable[i] >= mThreshold) {
                    mMultipleOutputs.write("candidates", NullWritable.get(), new Text(Integer.toString(i)));
                }
            }
            mMultipleOutputs.close();
        }
    }

    public static void run(int k, int n, int threshold, String input) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Configuration jConf = new Configuration(configuration);
        jConf.setInt("n", n);
        jConf.setInt("k", k);
        jConf.setInt("threshold", threshold);
        Job job = Job.getInstance(jConf, "Frequent Itemsets Pass " + k);
        job.setJarByClass(Main.class);
        job.setMapOutputKeyClass(PCYKey.class);
        job.setMapOutputValueClass(PCYValue.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(SolverMapper.class);
        job.setReducerClass(SolverReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path("frequent-itemsets-" + k));
        MultipleOutputs.addNamedOutput(job, "candidates", TextOutputFormat.class, NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "itemsets", TextOutputFormat.class, NullWritable.class, Text.class);
        job.waitForCompletion(true);
    }
}
