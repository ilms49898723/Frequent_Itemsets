package com.github.ilms49898723.frequentitemsets;

import com.github.ilms49898723.frequentitemsets.PCY.PCYFirstPass;
import com.github.ilms49898723.frequentitemsets.PCY.PCYSolver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

public class FrequentItemsetsMain extends Configured implements Tool {
    private int mK;
    private int mN;
    private int mThreshold;

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("usage: frequentitemsets <K> <N> <Threshold> <Input Dir>");
            return 1;
        }
        boolean errorFlag = false;
        if (!args[0].matches("[0-9]+")) {
            System.err.println("Invalid K: " + args[0]);
            errorFlag = true;
        }
        if (!args[1].matches("[0-9]+")) {
            System.err.println("Invalid N: " + args[1]);
            errorFlag = true;
        }
        if (!args[2].matches("[0-9]+")) {
            System.err.println("Invalid Threshold: " + args[2]);
            errorFlag = true;
        }
        if (errorFlag) {
            return 1;
        }
        mK = Integer.parseInt(args[0]);
        mN = Integer.parseInt(args[1]);
        mThreshold = Integer.parseInt(args[2]);
        cleanup();
        PCYFirstPass.run(1, mN, mThreshold, args[3]);
        for (int i = 2; i <= mK; ++i) {
            PCYSolver.run(i, mN, mThreshold, args[3]);
        }
        return 0;
    }

    private void cleanup() {
        try {
            FileSystem fileSystem = FileSystem.get(new Configuration());
            int index = 1;
            while (true) {
                if (fileSystem.exists(new Path("frequent-itemsets-" + index))) {
                    FileUtility.remove("frequent-itemsets-" + index, new Configuration());
                    ++index;
                } else {
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
