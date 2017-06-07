package com.github.ilms49898723.frequentitemsets;

import com.github.ilms49898723.frequentitemsets.PCY.PCYFirstPass;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

public class FrequentItemsetsMain extends Configured implements Tool {
    private int mK;
    private int mN;
    private int mThreshold;

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 5) {
            System.err.println("usage: frequentitemsets <K> <N> <Threshold> <Output Dir> <Input Dir>");
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
        FileUtility.remove("frequent-itemsets-1", new Configuration());
        PCYFirstPass.run(mK, mN, mThreshold, args[4]);
        return 0;
    }
}
