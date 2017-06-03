package com.github.ilms49898723.frequentitemsets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.*;

public class FileUtility {
    public static void mkdir(String name, Configuration conf) {
        try {
            FileSystem fileSystem = FileSystem.get(conf);
            fileSystem.mkdirs(new Path(name));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void touch(String name, Configuration conf) {
        try {
            FileSystem fileSystem = FileSystem.get(conf);
            fileSystem.create(new Path(name), true).close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void append(String destination, String source, Configuration conf) {
        try {
            FileSystem fileSystem = FileSystem.get(conf);
            Path in = new Path(source);
            Path out = new Path(destination);
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(fileSystem.open(in))
            );
            BufferedWriter writer = new BufferedWriter(
                    new OutputStreamWriter(fileSystem.append(out))
            );
            String line;
            while ((line = reader.readLine()) != null) {
                writer.write(line);
                writer.newLine();
            }
            reader.close();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void copyFile(String input, String output, Configuration conf) {
        try {
            FileSystem fileSystem = FileSystem.get(conf);
            Path in = new Path(input);
            Path out = new Path(output);
            FileUtil.copy(fileSystem, in, fileSystem, out, false, new Configuration());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void remove(String path, Configuration conf) {
        try {
            FileSystem fileSystem = FileSystem.get(conf);
            fileSystem.delete(new Path(path), true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String removeExtension(String path) {
        int index = path.lastIndexOf('.');
        if (index == -1 || index == 0) {
            return path;
        } else {
            return path.substring(0, index);
        }
    }

    public static String removeSlash(String path) {
        return path.replaceAll("/", "_");
    }
}
