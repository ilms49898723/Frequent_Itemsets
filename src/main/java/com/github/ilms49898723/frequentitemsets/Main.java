package com.github.ilms49898723.frequentitemsets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class Main {
    public static void main(String[] args) {
        try {
            int exitStatus = ToolRunner.run(
                    new Configuration(),
                    new FrequentItemsetsMain(),
                    args
            );
            System.exit(exitStatus);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
