package edu.jhu.fcriscu1.als.graphdb.poc;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/*
POC class to evaluate reading test data from a resource file
 */
public class ReadResourceFileTest {
    public ReadResourceFileTest(String fileName) {
        readFile(fileName);
    }
    private void readFile(String filename){
        Path path;
        {
            try {
                URL resource =  ReadResourceFileTest.class.getResource(filename);
               path =  Paths.get(resource.toURI());
               // path = Paths.get(this.getClass().getResource(filename).toURI());
                Files.readAllLines(path).stream().limit(1000).forEach(System.out::println);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String... args) {
       ReadResourceFileTest app = new ReadResourceFileTest("/TestFiles/test_human_intact.tsv");

    }


}
