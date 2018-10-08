package org.nygenome.als.graphdb.lib;


import com.google.common.base.Strings;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
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
import org.neo4j.graphdb.MultipleFoundException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.nygenome.als.graphdb.app.ALSDatabaseImportApp;
import org.nygenome.als.graphdb.app.ALSDatabaseImportApp.LabelTypes;
import org.nygenome.als.graphdb.app.ALSDatabaseImportApp.RelTypes;
import org.nygenome.als.graphdb.util.AsyncLoggingService;
import scala.Function3;
import scala.Tuple2;
import scala.collection.immutable.List;


public class FunctionLib {

  private static final Logger log = Logger.getLogger(FunctionLib.class);

  public FunctionLib() {
  }

  /*
  Public Function to determine if a specified ensembl id is for
  an esembl gene or an ensembl transcript
   */
  public Function<String,Optional<LabelTypes>> resolveEnsemblGeneticEntityLabelFunction = (id) -> {
    if (id.toUpperCase().startsWith("ENSG")) {
      return Optional.of(LabelTypes.EnsemblGene);
    }
    if (id.toUpperCase().startsWith("ENST")) {
      return Optional.of(LabelTypes.EnsemblTranscript);
    }
    return Optional.empty();
  };

  private final Supplier<Node> unknownNodeSupplier = () -> {
    try (Transaction tx = ALSDatabaseImportApp.INSTANCE.transactionSupplier.get()) {
      return ALSDatabaseImportApp.getGraphInstance().createNode(LabelTypes.Unknown);
    }
  };
/*
Public Function to find or create a Node
Parameters are the Node's label type, a key property, and a value for that property
The value must be a String
The return is a new or existing Node
 */

public Function3<Label,String,String, Node> resolveNodeFunction =
    (label, property, value) -> {

      Transaction tx = ALSDatabaseImportApp.INSTANCE.transactionSupplier.get();
      try {
        Node node = ALSDatabaseImportApp.getGraphInstance()
            .findNode(label,property,value);
        if (null == node) {
          node = ALSDatabaseImportApp.getGraphInstance().createNode(label);
          node.setProperty(property,value);
        }
        tx.success();
        return node;
      } catch (MultipleFoundException e) {
        tx.failure();
        AsyncLoggingService.logError("ERROR: multiple instances of Node " +label
          +"  " +property +"  " +value);
        AsyncLoggingService.logError(e.getMessage());
        e.printStackTrace();
      } finally {
        tx.close();
      }
      return unknownNodeSupplier.get();
    };

  /*
   A private Consumer that will add a Label to a Node if
   that Label is new
    */
  public BiConsumer<Node, Label> novelLabelConsumer = (node, newLabel) -> {
    Transaction tx = ALSDatabaseImportApp.INSTANCE.transactionSupplier.get();
    try {
      if (StreamSupport.stream(node.getLabels().spliterator(), false)
          .noneMatch(label -> label.name().equalsIgnoreCase(newLabel.name()))) {
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

  public Predicate<Node> isAlsAssociatedPredicate = (node) ->{
    Transaction tx = ALSDatabaseImportApp.INSTANCE.transactionSupplier.get();
    try {
      if (StreamSupport.stream(node.getLabels().spliterator(), false)
          .anyMatch(label -> label.name().equalsIgnoreCase("ALS-associated")))
      {
        tx.success();
        return true;
      }
      tx.success();
    } catch (Exception e) {
      tx.failure();
      e.printStackTrace();
    } finally {
      tx.close();
    }
    return false;
  };

  public BiConsumer<Relationship,Tuple2<String,Integer>> setRelationshipIntegerProperty = (rel, tuple)-> {
    Transaction tx = ALSDatabaseImportApp.INSTANCE.transactionSupplier.get();
    try {
      rel.setProperty(tuple._1(),tuple._2());
      tx.success();
    } catch (Exception e) {
      tx.failure();
      e.printStackTrace();
    } finally {
      tx.close();
    }
  };

  /*
  Public Function to find or create a Relationship between two nodes.
  n.b. if the relationship NodeA -> NodeB requires a different relationship type
  than NodeB -> NodeA, then two (2) relationships need to be created
   */
  public BiFunction<Tuple2<Node,Node>, RelationshipType,Relationship> resolveNodeRelationshipFunction = (nodeTuple, relType) -> {
    Transaction tx = ALSDatabaseImportApp.INSTANCE.transactionSupplier.get();
    Node nodeStart = nodeTuple._1();
    Node nodeEnd = nodeTuple._2();
    try {
     Relationship rel = StreamSupport.stream(nodeStart.getRelationships().spliterator(), false)
         .filter(relationship -> relationship.getEndNodeId() == nodeEnd.getId())
         .filter(relationship -> relationship.getType().name().equalsIgnoreCase(relType.name()) )
         .findFirst().orElse(nodeStart.createRelationshipTo(nodeEnd,relType));
     AsyncLoggingService.logInfo("created relationship between " +nodeStart.getLabels().toString()
     +" and " + nodeEnd.getLabels().toString());
      tx.success();
      return rel;
    } catch (Exception e) {
      tx.failure();
      e.printStackTrace();
    } finally {
      tx.close();
    }
    return null;
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

  public BiConsumer<Relationship, Tuple2<String,String>> relationshipPropertyValueConsumer = (relationship, propertyTuple) -> {
    if (!Strings.isNullOrEmpty(propertyTuple._2())) {
      Transaction tx = ALSDatabaseImportApp.INSTANCE.transactionSupplier.get();
      try {
        relationship.setProperty(propertyTuple._1(), propertyTuple._2());
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

  /*
  Protected BiConsumer to register an Array Strings as a property values for a specified node

   */
  public BiConsumer<Node, Tuple2<String, String[]>> nodePropertyValueStringArrayConsumer = (node, propertyListTuple) -> {
    if (propertyListTuple._2() != null && propertyListTuple._2().length > 0) {
      Transaction tx = ALSDatabaseImportApp.INSTANCE.transactionSupplier.get();
      try {
        node.setProperty(propertyListTuple._1(), propertyListTuple._2());
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

