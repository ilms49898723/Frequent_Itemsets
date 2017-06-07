package com.github.ilms49898723.frequentitemsets.PCY;

import java.util.List;

public class PCYUtil {
    public static int hash(int[] items, int mod) {
        int value = 1;
        int base = 1;
        for (int item : items) {
            value += item * base;
            base = base * base;
        }
        value = value % mod;
        return Math.abs(value);
    }

    public static int hash(List<Integer> items, int mod) {
        int value = 1;
        int base = 1;
        for (int item : items) {
            value += item * base;
            base = base * base;
        }
        value = value % mod;
        return Math.abs(value);
    }
}
