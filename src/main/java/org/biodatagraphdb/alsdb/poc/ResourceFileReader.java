package org.biodatagraphdb.alsdb.poc;

import com.google.common.io.Resources;

import javax.annotation.Nonnull;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.stream.Stream;

/*
POC class to evaluate reading test data from a resource file
 */
public class ResourceFileReader {
    private String testFilename;
    public ResourceFileReader(@Nonnull  String fileName) {
       testFilename = fileName;
    }

    public Stream<String> readTestDataFile(){

        {
            try {
                return Resources.readLines(Resources.getResource(testFilename),StandardCharsets.UTF_8 ).stream();

                //ClassLoader classloader = Thread.currentThread().getContextClassLoader();
//                InputStream is = ResourceFileReader.class.getResourceAsStream(testFilename);
//                InputStreamReader streamReader = new InputStreamReader(is, StandardCharsets.UTF_8);
//                BufferedReader reader = new BufferedReader(streamReader);
//                return reader.lines();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return Stream.empty();
    }

    public static void main(String... args) {
        ResourceFileReader app = new ResourceFileReader("/TestFiles/test-uniprot-human.tsv");
        app.readTestDataFile().limit(100)
                .forEach(System.out::println);

    }


}
