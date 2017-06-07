package com.github.ilms49898723.frequentitemsets.PCY;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

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
}
