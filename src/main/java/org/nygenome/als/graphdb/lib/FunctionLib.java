package org.nygenome.als.graphdb.lib;


import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class FunctionLib {
    private static final Logger log = Logger.getLogger(FunctionLib.class);

    public Function<String, Path> readResourceFileFunction = (fileName) -> {
        try {
            ClassLoader cl = getClass().getClassLoader();
            URI uri = cl.getResource(fileName).toURI();
            return Paths.get(uri);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
        return null;
    };

    public static Stream<String> generateLineStreamFromPath(Path path) {
        try {
            return Files.lines(path);
        } catch (IOException e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }
        return Stream.empty();
    }

    public static Function<String[],String[]> processTokensFunction = (tokens)-> {
       return (String[])  Arrays.stream(tokens).map(token -> token.toUpperCase().trim())
                .collect(Collectors.toList()).toArray();
    };

    public static void main(String... args) {
        Path path = new FunctionLib().readResourceFileFunction.apply
                ("intact.txt");
        log.info("File exists = " + path.toString());
        log.info("There are " + FunctionLib.generateLineStreamFromPath(path).count()
                + " lines in file " + path.toString());

    }
}

