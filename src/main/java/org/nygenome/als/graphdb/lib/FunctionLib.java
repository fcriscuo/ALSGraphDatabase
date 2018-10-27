package org.nygenome.als.graphdb.lib;

import com.google.common.base.Strings;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;
import org.apache.log4j.Logger;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.MultipleFoundException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.nygenome.als.graphdb.app.ALSDatabaseImportApp;
import org.nygenome.als.graphdb.app.ALSDatabaseImportApp.LabelTypes;
import org.nygenome.als.graphdb.supplier.GraphDatabaseServiceSupplier;
import org.nygenome.als.graphdb.supplier.GraphDatabaseServiceSupplier.RunMode;
import org.nygenome.als.graphdb.util.AsyncLoggingService;
import scala.Tuple2;
import scala.Tuple3;
import scala.collection.immutable.List;

public class FunctionLib {

  private static final Logger log = Logger.getLogger(FunctionLib.class);
  private GraphDatabaseService graphDb;
  public FunctionLib(@Nonnull  RunMode runMode) {
    this.graphDb = new GraphDatabaseServiceSupplier(runMode).get();
  }
  /*
  ppRel.setProperty("Interaction_method_detection",
        ppi.detectionMethodList().mkString("|"));
   */
  public BiConsumer<Relationship,Tuple2<String,String>> defineRelationshipPropertyConsumer
      = (rel,tuple2) -> {
    try (Transaction tx = graphDb.beginTx()) {
      rel.setProperty(tuple2._1(),tuple2._2());
    }
  };

  /*
  Public Function to determine if a specified ensembl id is for
  an esembl gene or an ensembl transcript
   */
  public Function<String, Optional<LabelTypes>> resolveEnsemblGeneticEntityLabelFunction = (id) -> {
    if (id.toUpperCase().startsWith("ENSG")) {
      return Optional.of(LabelTypes.EnsemblGene);
    }
    if (id.toUpperCase().startsWith("ENST")) {
      return Optional.of(LabelTypes.EnsemblTranscript);
    }
    return Optional.empty();
  };

  private final Supplier<Node> unknownNodeSupplier = () -> {
    try (Transaction tx = graphDb.beginTx()) {
      return this.graphDb.createNode(LabelTypes.Unknown);
    }
  };

  /*
  Public Function to find an existing Node
  Returns an Optional to accommodate non-existent Nodes
   */
  public Function<Tuple3<Label, String, String>, Optional<Node>> findExistingGraphNodeFunction = (tuple3) -> {
    Label label = tuple3._1();
    String property = tuple3._2();
    String value = tuple3._3();
    Transaction tx = this.graphDb.beginTx();
    try {
      Node node = this.graphDb
          .findNode(label, property, value);
      tx.success();
      return (node != null) ? Optional.of(node) : Optional.empty();
    } catch (MultipleFoundException e) {
      tx.failure();
      AsyncLoggingService.logError("ERROR: multiple instances of Node " + label
          + "  " + property + "  " + value);
      AsyncLoggingService.logError(e.getMessage());
      e.printStackTrace();
    } finally {
      tx.close();
    }
    return Optional.empty();
  };


  /*
  Public Predicate to determine if a unique Node exists in the database
  The specified property must have unique values (i.e. key)
   */
  public Predicate<Tuple3<Label, String, String>> isExistingNodePredicate = (tuple3) ->
  {
    Transaction tx = this.graphDb.beginTx();
    try {
      return (
          this.graphDb.findNode(tuple3._1(), tuple3._2(), tuple3._3())
              != null);
    } catch (MultipleFoundException e) {
      e.printStackTrace();
    } finally {
      tx.close();
    }
    return false;
  };

  /*
  Private function that will create a new graph node based on the supplied
  Label, property, & property value
  This method should only be invoked after confirming that the Node does not already exist
   */
  private Function<Tuple3<Label, String, String>, Node> createGraphNodeFunction = (tuple3) -> {
    Label label = tuple3._1();
    String property = tuple3._2();
    String value = tuple3._3();
    Transaction tx = this.graphDb.beginTx();
    try {
      Node node = this.graphDb.createNode(label);
      node.setProperty(property, value);
      tx.success();
      return node;
    } catch (Exception e) {
      tx.failure();
      AsyncLoggingService.logError("ERROR: creating Node " + label
          + "  " + property + "  " + value);
      AsyncLoggingService.logError(e.getMessage());
      e.printStackTrace();
    } finally {
      tx.close();
    }
    return unknownNodeSupplier.get();
  };

  /*
  Public Function that will find or create a graph Node based on its Label,
  key property, and String property value
   */
  public Function<Tuple3<Label, String, String>, Node> resolveGraphNodeFunction =
      (tuple3) -> {
       Optional<Node> nodeOpt=  findExistingGraphNodeFunction.apply(tuple3);
       if(nodeOpt.isPresent()) {
         return nodeOpt.get();
       }
       return createGraphNodeFunction.apply(tuple3);
      };

/*
Public Function to find or create a Node
Parameters are the Node's label type, a key property, and a value for that property
The value must be a String
The return is a new or existing Node
 */

  public Function<Tuple3<Label, String, String>, Node> resolveGraphNodeFunctionOld =
      (tuple3) -> {
        Label label = tuple3._1();
        String property = tuple3._2();
        String value = tuple3._3();
        Transaction tx = this.graphDb.beginTx();
        try {
          Node node = this.graphDb
              .findNode(label, property, value);
          if (null == node) {
            node = this.graphDb.createNode(label);
            node.setProperty(property, value);
          }
          tx.success();
          return node;
        } catch (MultipleFoundException e) {
          tx.failure();
          AsyncLoggingService.logError("ERROR: multiple instances of Node " + label
              + "  " + property + "  " + value);
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
    Transaction tx = this.graphDb.beginTx();
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

  public Predicate<Node> isAlsAssociatedPredicate = (node) -> {
    Transaction tx = this.graphDb.beginTx();
    try {
      if (StreamSupport.stream(node.getLabels().spliterator(), false)
          .anyMatch(label -> label.name().equalsIgnoreCase("ALS-associated"))) {
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

  public BiConsumer<Relationship, Tuple2<String, Integer>> setRelationshipIntegerProperty = (rel, tuple) -> {
    Transaction tx = this.graphDb.beginTx();
    try {
      rel.setProperty(tuple._1(), tuple._2());
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
  public BiFunction<Tuple2<Node, Node>, RelationshipType, Relationship> resolveNodeRelationshipFunction = (nodeTuple, relType) -> {
    Transaction tx = this.graphDb.beginTx();
    Node nodeStart = nodeTuple._1();
    Node nodeEnd = nodeTuple._2();
    try {
      Relationship rel = StreamSupport.stream(nodeStart.getRelationships().spliterator(), false)
          .filter(relationship -> relationship.getEndNodeId() == nodeEnd.getId())
          .filter(relationship -> relationship.getType().name().equalsIgnoreCase(relType.name()))
          .findFirst().orElse(nodeStart.createRelationshipTo(nodeEnd, relType));
      AsyncLoggingService.logDebug("created relationship between " + nodeStart.getLabels().toString()
          + " and " + nodeEnd.getLabels().toString());
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
      Transaction tx = this.graphDb.beginTx();
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

  public BiConsumer<Relationship, Tuple2<String, String>> relationshipPropertyValueConsumer = (relationship, propertyTuple) -> {
    if (!Strings.isNullOrEmpty(propertyTuple._2())) {
      Transaction tx = this.graphDb.beginTx();
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
      Transaction tx = this.graphDb.beginTx();
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
      Transaction tx = this.graphDb.beginTx();
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
      Transaction tx = this.graphDb.beginTx();
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
    Path path = new FunctionLib(RunMode.READ_ONLY).readResourceFileFunction.apply
        ("intact.txt");
    log.info("File exists = " + path.toString());
    log.info("There are " + FunctionLib.generateLineStreamFromPath(path).count()
        + " lines in file " + path.toString());

  }

}

