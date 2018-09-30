package org.nygenome.als.graphdb.lib;


import com.google.common.base.Strings;
import java.util.function.BiConsumer;
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
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.nygenome.als.graphdb.EmbeddedGraph;
import org.nygenome.als.graphdb.EmbeddedGraph.LabelTypes;
import org.nygenome.als.graphdb.util.AsyncLoggingService;
import scala.Tuple2;
import scala.collection.immutable.List;


public class FunctionLib {
    private static final Logger log = Logger.getLogger(FunctionLib.class);
    public FunctionLib(){}

    public Function<String, LabelTypes> resolveGeneOntologyPrincipleFunction = (princ) -> {
        if (princ.toUpperCase().startsWith("MOLECULAR")) {
            return LabelTypes.MolecularFunction;
        }
        if (princ.toUpperCase().startsWith("BIOLOGICAL")) {
            return LabelTypes.BiologicalProcess;
        }
        if (princ.toUpperCase().startsWith("CELLULAR")) {
            return LabelTypes.CellularComponents;
        }
        AsyncLoggingService.logError(princ + " is not a valid Gene Ontology principle ");
        return LabelTypes.Unknown;
    };


    /*
 Protected BiConsumer that will add a property name/value pair to a specified node
 Currently only String property values are supported
  */
    public  BiConsumer<Node, Tuple2<String, String>> nodePropertyValueConsumer = (node, propertyTuple) -> {
        if (!Strings.isNullOrEmpty(propertyTuple._2())) {
            Transaction tx = EmbeddedGraph.INSTANCE.transactionSupplier.get();
            try {
                node.setProperty(propertyTuple._1(), propertyTuple._2());
                tx.success();
            } catch (Exception e) {
                tx.failure();
                AsyncLoggingService.logError(e.getMessage());
            } finally {
                tx.close();
            }
        }
    };

    /*
    Protected BiConsumer to register a List of property values for a specified node
    Property values are persisted as Strings
     */
    public BiConsumer<Node, Tuple2<String, List<String>>> nodePropertyValueListConsumer = (node, propertyListTuple) -> {
        if (propertyListTuple._2() != null && propertyListTuple._2().size() > 0) {
            Transaction tx = EmbeddedGraph.INSTANCE.transactionSupplier.get();
            try {
                node.setProperty(propertyListTuple._1(), propertyListTuple._2().head());
                tx.success();
            }  catch (Exception e) {
                tx.failure();
                AsyncLoggingService.logError(e.getMessage());
            } finally {
                tx.close();
            }
        }
    };


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

    @Deprecated
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

