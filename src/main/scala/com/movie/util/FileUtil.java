package com.movie.util;

import java.io.*;

public class FileUtil {

    static BufferedWriter bw = null;
//"/Users/umeng/Documents/bigdata/ml-latest-small/predict.csv"
    public static void init(String fileName) {
        try {
            File csv1 = new File(fileName);
            bw = new BufferedWriter(new FileWriter(csv1));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static BufferedWriter getWriter() {
        return bw;
    }

}
