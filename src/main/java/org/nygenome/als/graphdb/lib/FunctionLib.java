package org.nygenome.als.graphdb.lib;


import com.google.common.base.Strings;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.StreamSupport;
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
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.nygenome.als.graphdb.app.ALSDatabaseImportApp;
import org.nygenome.als.graphdb.app.ALSDatabaseImportApp.LabelTypes;
import org.nygenome.als.graphdb.app.ALSDatabaseImportApp.RelTypes;
import org.nygenome.als.graphdb.util.AsyncLoggingService;
import scala.Tuple2;
import scala.collection.immutable.List;


public class FunctionLib {

  private static final Logger log = Logger.getLogger(FunctionLib.class);

  public FunctionLib() {
  }

  /*
   A private Consumer that will add a Label to a Node if
   that Label is new
    */
  public BiConsumer<Node, Label> novelLabelConsumer = (node, newLabel) -> {
    Transaction tx = ALSDatabaseImportApp.INSTANCE.transactionSupplier.get();
    try {
      if (!StreamSupport.stream(node.getLabels().spliterator(), false)
          .anyMatch(label -> label.name().equalsIgnoreCase(newLabel.name()))) {
        node.addLabel(newLabel);
      }
      tx.success();
    } catch (Exception e) {
      tx.failure();
      e.printStackTrace();
    } finally {
      tx.close();
    }
  };

  /*
Protected method to create two (2) directional relationships between two (2)
specified nodes. The first relationship is registered in a Map to prevent
duplication
The relationship types are also provided
The created or existing Relationships are returned in a Tuple2 in the A->B, & B->A order
 */
  public Tuple2<Relationship, Relationship> createBiDirectionalRelationship(Node nodeA, Node nodeB,
      Tuple2<String, String> keyTuple,
      Map<Tuple2<String, String>, Relationship> relMap, RelTypes relTypeA, RelTypes relTypeB) {
    if (!relMap.containsKey(keyTuple)) {
      Transaction tx = ALSDatabaseImportApp.INSTANCE.transactionSupplier.get();
      try {
        Relationship relA = nodeA.createRelationshipTo(nodeB, relTypeA);
        Relationship relB = nodeB.createRelationshipTo(nodeA, relTypeB);
        relMap.put(keyTuple, relA);
        relMap.put(keyTuple.swap(), relB);
        tx.success();
        AsyncLoggingService.logInfo("Created realtionship between " + keyTuple._1() + " and "
            + keyTuple._2());
      } catch (Exception e) {
        tx.failure();
        AsyncLoggingService.logError(
            "ERR: failed to create bi-directional realtionship between " + keyTuple._1() + " and "
                + keyTuple._2());
        e.printStackTrace();
        return null;
      } finally {
        tx.close();
      }
    }
    return new Tuple2<>(relMap.get(keyTuple), relMap.get(keyTuple.swap()));
  }


  // public utility method to create a uni-directional relationship from Node A to Node B
  public Relationship createUniDirectionalRelationship(Node nodeA, Node nodeB,
      Tuple2<String, String> keyTuple,
      Map<Tuple2<String, String>, Relationship> relMap, RelTypes relTypeA) {
    if (!relMap.containsKey(keyTuple)) {
      Transaction tx = ALSDatabaseImportApp.INSTANCE.transactionSupplier.get();
      try {
        Relationship relA = nodeA.createRelationshipTo(nodeB, relTypeA);
        relMap.put(keyTuple, relA);
        tx.success();
        AsyncLoggingService
            .logInfo("Created uni-directional relationship from " + keyTuple._1() + " to "
                + keyTuple._2());
      } catch (Exception e) {
        tx.failure();
        AsyncLoggingService.logError(
            "ERR: failed to create uni-directional realtionship between " + keyTuple._1() + " and "
                + keyTuple._2());
        e.printStackTrace();
        return null;
      } finally {
        tx.close();
      }
    }
    return relMap.get(keyTuple);
  }

  /*
  Protected BiConsumer that accepts a Pair of Realtionships and a property key/value pair
  The supplied property is applied to each of the Relationships
   */
  public BiConsumer<Tuple2<Relationship, Relationship>, Tuple2<String, String>> relationshipPairPropertyConsumer
      = (relPair, keyValue) -> {
    relPair._1().setProperty(keyValue._1(), keyValue._2());
    relPair._2().setProperty(keyValue._1(), keyValue._2());
  };

  public Function<String, LabelTypes> resolveGeneOntologyPrincipleFunction = (princ) -> {
    if (princ.toUpperCase().startsWith("MOLECULAR")) {
      return LabelTypes.MolecularFunction;
    }
    if (princ.toUpperCase().startsWith("BIO")) {
      return LabelTypes.BiologicalProcess;
    }
    if (princ.toUpperCase().startsWith("CELLULAR")) {
      return LabelTypes.CellularComponents;
    }
    AsyncLoggingService.logError(princ + " is not a valid Gene Ontology principle ");
    return LabelTypes.Unknown;
  };


  /*
Protected BiConsumer that will add a property name/String value pair to a specified node

*/
  public BiConsumer<Node, Tuple2<String, String>> nodePropertyValueConsumer = (node, propertyTuple) -> {
    if (!Strings.isNullOrEmpty(propertyTuple._2())) {
      Transaction tx = ALSDatabaseImportApp.INSTANCE.transactionSupplier.get();
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
Protected BiConsumer that will add a property name/String value pair to a specified node

*/
  public BiConsumer<Node, Tuple2<String, Integer>> nodeIntegerPropertyValueConsumer = (node, propertyTuple) -> {
    if (null != propertyTuple._2()) {
      Transaction tx = ALSDatabaseImportApp.INSTANCE.transactionSupplier.get();
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
      Transaction tx = ALSDatabaseImportApp.INSTANCE.transactionSupplier.get();
      try {
        node.setProperty(propertyListTuple._1(), propertyListTuple._2().head());
        tx.success();
      } catch (Exception e) {
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
  public static Function<String[], String[]> processTokensFunction = (tokens) -> {
    return (String[]) Arrays.stream(tokens).map(token -> token.toUpperCase().trim())
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

