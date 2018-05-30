package org.nygenome.als.graphdb.lib;

import java.io.*;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;
import java.util.stream.Stream;

public class Hello {

    public static void main(String[] args) {
        Hello obj = new Hello();
        System.out.println(obj.getFile("/files/intact_negative.txt"));
    }

    private String getFile(String fileName) {

        StringBuilder result = new StringBuilder("");

        //Get file from resources folder
        try {

            Path p = Paths.get(ClassLoader.getSystemResource(fileName).toURI());
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        // ClassLoader classLoader = ClassLoader.getSystemClassLoader();
       // InputStream is = classLoader.getResourceAsStream(fileName);
       // File file = new File(classLoader.getResource(fileName).getFile());
       return null;
    }

}