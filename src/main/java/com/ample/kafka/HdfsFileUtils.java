package com.ample.kafka;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.fs.FSDataOutputStream;

import org.apache.hadoop.io.IOUtils;

import java.io.*;

/**
 * Created by Dhwanit on 15/05/16.
 */
public final class HdfsFileUtils {

    public static void moveFileToHdfs(String path) {

        Configuration hadoopconf = new Configuration();
        hadoopconf.addResource(new Path("core-site.xml"));
        hadoopconf.addResource(new Path("hdfs-site.xml"));
        try {
            FileSystem fs = FileSystem.get(hadoopconf);
            FSDataOutputStream outFileStream = fs.create(new Path("/" + path));

            InputStream is = new BufferedInputStream(new FileInputStream(path));
            IOUtils.copyBytes(is, outFileStream, hadoopconf);

        } catch (IOException e) {
            System.out.println("NOT COPIED" + e.getMessage());
        }
    }
}
