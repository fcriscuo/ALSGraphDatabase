package org.biodatagraphdb.alsdb.lib;

import com.google.common.base.Strings;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
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
import org.biodatagraphdb.alsdb.service.graphdb.AlsGraphDatabaseSupplier;
import org.biodatagraphdb.alsdb.service.graphdb.RunMode;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.MultipleFoundException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.biodatagraphdb.alsdb.util.AsyncLoggingService;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import scala.Tuple2;
import scala.Tuple3;
import org.biodatagraphdb.alsdb.app.ALSDatabaseImportApp.LabelTypes;

public class FunctionLib {

	private File DEFAULT_TEST_DATABSE_DIRECTORY = new File("target/test-als-graph-db");
	private static final Logger log = Logger.getLogger(FunctionLib.class);
	private GraphDatabaseService graphDb;
	public FunctionLib(@Nonnull RunMode runMode) {
		this.graphDb = new AlsGraphDatabaseSupplier(DEFAULT_TEST_DATABSE_DIRECTORY,
				GraphDatabaseSettings.DEFAULT_DATABASE_NAME,runMode).get();
	}

	public void shutDown() {
		AsyncLoggingService.logInfo("Shutting down database ...");
		//TODO: fix shut down issue
		//graphDb.beginTx();
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
			return tx.createNode(LabelTypes.Unknown);
		}
	};

	/*
	Public Function to find an existing Node
	Returns an Optional to accommodate non-existent Nodes
	 */
	public Function<Tuple3<Label, String, String>, Optional<Node>> findExistingGraphNodeFunction = (tuple3) -> {
		final Label label = tuple3._1();
		final String property = tuple3._2();
		final String value = tuple3._3();
		Transaction tx = this.graphDb.beginTx();
		try {
			Node node = tx
				.findNode(label, property, value);
			tx.commit();
			return (node != null) ? Optional.of(node) : Optional.empty();
		} catch (MultipleFoundException e) {
			tx.rollback();
			AsyncLoggingService.logError("ERROR: multiple instances of Node " + label
				+ "  " + property + "  " + value);
			AsyncLoggingService.logError(e.getMessage());
			e.printStackTrace();
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
				tx.findNode(tuple3._1(), tuple3._2(), tuple3._3())
					!= null);
		} catch (MultipleFoundException e) {
			e.printStackTrace();
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
			Node node = tx.createNode(label);
			node.setProperty(property, value);
			tx.commit();
			return node;
		} catch (Exception e) {
			tx.rollback();
			AsyncLoggingService.logError("ERROR: creating Node " + label
				+ "  " + property + "  " + value);
			AsyncLoggingService.logError(e.getMessage());
			e.printStackTrace();
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
			tx.commit();
		} catch (Exception e) {
			tx.rollback();
			e.printStackTrace();
		}
	};

	public Predicate<Node> isAlsAssociatedPredicate = (node) -> {
		Transaction tx = this.graphDb.beginTx();
		try {
			if (StreamSupport.stream(node.getLabels().spliterator(), false)
				.anyMatch(label -> label.name().equalsIgnoreCase("ALS-associated"))) {
				tx.commit();
				return true;
			}
			tx.commit();
		} catch (Exception e) {
			tx.rollback();
			e.printStackTrace();
		}
		return false;
	};

	public BiConsumer<Relationship, Tuple2<String, Integer>> setRelationshipIntegerProperty = (rel, tuple) -> {
		Transaction tx = this.graphDb.beginTx();
		try {
			rel.setProperty(tuple._1(), tuple._2());
			tx.commit();
		} catch (Exception e) {
			tx.rollback();
			e.printStackTrace();
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
			tx.commit();
			return rel;
		} catch (Exception e) {
			tx.rollback();
			e.printStackTrace();
		}
		return null;
	};


	public Function<String, LabelTypes> resolveGeneOntologyPrincipleFunction = (princ) -> {
		if (princ.toUpperCase().startsWith("MOLECULAR")) {
			return LabelTypes.MolecularFunction;
		}
		if (princ.toUpperCase().startsWith("BIO")) {
			return org.biodatagraphdb.alsdb.app.ALSDatabaseImportApp.LabelTypes.BiologicalProcess;
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
				tx.commit();
			} catch (Exception e) {
				tx.rollback();
				AsyncLoggingService.logError(e.getMessage());
			}
		}
	};

	public BiConsumer<Relationship, Tuple2<String, String>> relationshipPropertyValueConsumer = (relationship, propertyTuple) -> {
		if (!Strings.isNullOrEmpty(propertyTuple._2())) {
			Transaction tx = this.graphDb.beginTx();
			try {
				relationship.setProperty(propertyTuple._1(), propertyTuple._2());
				tx.commit();
			} catch (Exception e) {
				tx.rollback();
				AsyncLoggingService.logError(e.getMessage());
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
				tx.commit();
			} catch (Exception e) {
				tx.rollback();
				AsyncLoggingService.logError(e.getMessage());
			}
		}
	};
	/*
	Protected BiConsumer to register a List of property values for a specified node
	Property values are persisted as Strings
	 */
	public BiConsumer<Node, Tuple2<String, List<String>>> nodePropertyValueListConsumer = (node, propertyListTuple) -> {
		if (!propertyListTuple._2().isEmpty()) {
			Transaction tx = this.graphDb.beginTx();
			try {
				node.setProperty(propertyListTuple._1(), propertyListTuple._2().get(0));
				tx.commit();
			} catch (Exception e) {
				tx.rollback();
				AsyncLoggingService.logError(e.getMessage());
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
				tx.commit();
			} catch (Exception e) {
				tx.rollback();
				AsyncLoggingService.logError(e.getMessage());
			} 
		}
	};

	/*
	Protected BiConsumer to register a List of Strings as a property values for a specified node
	 */
	public BiConsumer<Node, Tuple2<String, java.util.List<String>>> nodePropertyValueStringListConsumer = (node, propertyListTuple) -> {
		if (!propertyListTuple._2().isEmpty()) {
			Transaction tx = this.graphDb.beginTx();
			try {
				node.setProperty(propertyListTuple._1(), propertyListTuple._2());
				tx.commit();
			} catch (Exception e) {
				tx.rollback();
				AsyncLoggingService.logError(e.getMessage());
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
			("testhumanintact.txt");
		log.info("File exists = " + path.toString());
		log.info("There are " + FunctionLib.generateLineStreamFromPath(path).count()
			+ " lines in file " + path.toString());

	}

}
