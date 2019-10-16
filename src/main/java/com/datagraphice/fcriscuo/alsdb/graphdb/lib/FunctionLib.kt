package edu.jhu.fcriscu1.als.graphdb.lib

import com.google.common.base.Strings
import edu.jhu.fcriscu1.als.graphdb.app.ALSDatabaseImportApp.LabelTypes
import edu.jhu.fcriscu1.als.graphdb.supplier.GraphDatabaseServiceSupplier
import edu.jhu.fcriscu1.als.graphdb.util.AsyncLoggingService
import org.apache.log4j.Logger
import org.neo4j.graphdb.*
import scala.collection.immutable.List
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.*
import java.util.stream.Collectors
import java.util.stream.Stream
import java.util.stream.StreamSupport

class FunctionLib(runMode: GraphDatabaseServiceSupplier.RunMode) {
    private val graphDb: GraphDatabaseService = GraphDatabaseServiceSupplier(runMode).get()
    /*
  ppRel.setProperty("Interaction_method_detection",
        ppi.detectionMethodList().mkString("|"));
   */
    var defineRelationshipPropertyConsumer = { rel, tuple2 -> graphDb.beginTx().use { tx -> rel.setProperty(tuple2._1(), tuple2._2()) } }

    /*
  Public Function to determine if a specified ensembl id is for
  an esembl gene or an ensembl transcript
   */
    var resolveEnsemblGeneticEntityLabelFunction = { id:String ->
        if (id.toUpperCase().startsWith("ENSG")) {
            Optional.of(LabelTypes.EnsemblGene)
        }
        if (id.toUpperCase().startsWith("ENST")) {
            Optional.of(LabelTypes.EnsemblTranscript)
        }
        Optional.empty<LabelTypes>()
    }

    private val unknownNodeSupplier = { graphDb.beginTx().use { tx -> return this.graphDb.createNode(LabelTypes.Unknown) } }

    /*
  Public Function to find an existing Node
  Returns an Optional to accommodate non-existent Nodes
   */
    var findExistingGraphNodeFunction = { tuple3 ->
        val label = tuple3._1()
        val property = tuple3._2()
        val value = tuple3._3()
        val tx = this.graphDb.beginTx()
        try {
            val node = this.graphDb
                    .findNode(label, property, value)
            tx.success()
            return if (node != null) Optional.of(node) else Optional.empty()
        } catch (e: MultipleFoundException) {
            tx.failure()
            AsyncLoggingService.logError("ERROR: multiple instances of Node " + label
                    + "  " + property + "  " + value)
            AsyncLoggingService.logError(e.message)
            e.printStackTrace()
        } finally {
            tx.close()
        }
        Optional.empty<Node>()
    }


    /*
  Public Predicate to determine if a unique Node exists in the database
  The specified property must have unique values (i.e. key)
   */
    var isExistingNodePredicate = { tuple3 ->
        val tx = this.graphDb.beginTx()
        try {
            return this.graphDb.findNode(tuple3._1(), tuple3._2(), tuple3._3()) != null
        } catch (e: MultipleFoundException) {
            e.printStackTrace()
        } finally {
            tx.close()
        }
        false
    }

    /*
  Private function that will create a new graph node based on the supplied
  Label, property, & property value
  This method should only be invoked after confirming that the Node does not already exist
   */
    private val createGraphNodeFunction = { tuple3 ->
        val label = tuple3._1()
        val property = tuple3._2()
        val value = tuple3._3()
        val tx = this.graphDb.beginTx()
        try {
            val node = this.graphDb.createNode(label)
            node.setProperty(property, value)
            tx.success()
            return node
        } catch (e: Exception) {
            tx.failure()
            AsyncLoggingService.logError("ERROR: creating Node " + label
                    + "  " + property + "  " + value)
            AsyncLoggingService.logError(e.message)
            e.printStackTrace()
        } finally {
            tx.close()
        }
        unknownNodeSupplier.get()
    }

    /*
  Public Function that will find or create a graph Node based on its Label,
  key property, and String property value
   */
    var resolveGraphNodeFunction = { tuple3 ->
        val nodeOpt = findExistingGraphNodeFunction.apply(tuple3)
        if (nodeOpt.isPresent()) {
            return nodeOpt.get()
        }
        createGraphNodeFunction.apply(tuple3)
    }


    /*
   A private Consumer that will add a Label to a Node if
   that Label is new
    */
    var novelLabelConsumer = { node, newLabel ->
        val tx = this.graphDb.beginTx()
        try {
            if (StreamSupport.stream<Label>(node.getLabels().spliterator(), false)
                            .noneMatch { label -> label.name().equals(newLabel.name(), ignoreCase = true) }) {
                node.addLabel(newLabel)
            }
            tx.success()
        } catch (e: Exception) {
            tx.failure()
            e.printStackTrace()
        } finally {
            tx.close()
        }
    }

    var isAlsAssociatedPredicate = { node ->
        val tx = this.graphDb.beginTx()
        try {
            if (StreamSupport.stream<Label>(node.getLabels().spliterator(), false)
                            .anyMatch { label -> label.name().equals("ALS-associated", ignoreCase = true) }) {
                tx.success()
                return true
            }
            tx.success()
        } catch (e: Exception) {
            tx.failure()
            e.printStackTrace()
        } finally {
            tx.close()
        }
        false
    }

    var setRelationshipIntegerProperty = { rel, tuple ->
        val tx = this.graphDb.beginTx()
        try {
            rel.setProperty(tuple._1(), tuple._2())
            tx.success()
        } catch (e: Exception) {
            tx.failure()
            e.printStackTrace()
        } finally {
            tx.close()
        }
    }

    /*
  Public Function to find or create a Relationship between two nodes.
  n.b. if the relationship NodeA -> NodeB requires a different relationship type
  than NodeB -> NodeA, then two (2) relationships need to be created
   */
    var resolveNodeRelationshipFunction = { nodeTuple, relType ->
        val tx = this.graphDb.beginTx()
        val nodeStart = nodeTuple._1()
        val nodeEnd = nodeTuple._2()
        try {
            val rel = StreamSupport.stream<Relationship>(nodeStart.getRelationships().spliterator(), false)
                    .filter { relationship -> relationship.endNodeId == nodeEnd.getId() }
                    .filter { relationship -> relationship.type.name().equals(relType.name(), ignoreCase = true) }
                    .findFirst().orElse(nodeStart.createRelationshipTo(nodeEnd, relType))
            tx.success()
            return rel
        } catch (e: Exception) {
            tx.failure()
            e.printStackTrace()
        } finally {
            tx.close()
        }
        null
    }


    var resolveGeneOntologyPrincipleFunction = { princ ->
        if (princ.toUpperCase().startsWith("MOLECULAR")) {
            return LabelTypes.MolecularFunction
        }
        if (princ.toUpperCase().startsWith("BIO")) {
            return LabelTypes.BiologicalProcess
        }
        if (princ.toUpperCase().startsWith("CELLULAR")) {
            return LabelTypes.CellularComponents
        }
        AsyncLoggingService.logError(princ + " is not a valid Gene Ontology principle ")
        LabelTypes.Unknown
    }


    /*
Protected BiConsumer that will add a property name/String value pair to a specified node

*/
    var nodePropertyValueConsumer = { node, propertyTuple ->
        if (!Strings.isNullOrEmpty(propertyTuple._2())) {
            val tx = this.graphDb.beginTx()
            try {
                node.setProperty(propertyTuple._1(), propertyTuple._2())
                tx.success()
            } catch (e: Exception) {
                tx.failure()
                AsyncLoggingService.logError(e.message)
            } finally {
                tx.close()
            }
        }
    }

    var relationshipPropertyValueConsumer = { relationship, propertyTuple ->
        if (!Strings.isNullOrEmpty(propertyTuple._2())) {
            val tx = this.graphDb.beginTx()
            try {
                relationship.setProperty(propertyTuple._1(), propertyTuple._2())
                tx.success()
            } catch (e: Exception) {
                tx.failure()
                AsyncLoggingService.logError(e.message)
            } finally {
                tx.close()
            }
        }
    }

    /*
Protected BiConsumer that will add a property name/String value pair to a specified node

*/
    var nodeIntegerPropertyValueConsumer = { node, propertyTuple ->
        if (null != propertyTuple._2()) {
            val tx = this.graphDb.beginTx()
            try {
                node.setProperty(propertyTuple._1(), propertyTuple._2())
                tx.success()
            } catch (e: Exception) {
                tx.failure()
                AsyncLoggingService.logError(e.message)
            } finally {
                tx.close()
            }
        }
    }
    /*
  Protected BiConsumer to register a List of property values for a specified node
  Property values are persisted as Strings
   */
    var nodePropertyValueListConsumer = { node, propertyListTuple ->
        if (propertyListTuple._2() != null && propertyListTuple._2().size() > 0) {
            val tx = this.graphDb.beginTx()
            try {
                node.setProperty(propertyListTuple._1(), propertyListTuple._2().head())
                tx.success()
            } catch (e: Exception) {
                tx.failure()
                AsyncLoggingService.logError(e.message)
            } finally {
                tx.close()
            }
        }
    }

    /*
  Protected BiConsumer to register an Array Strings as a property values for a specified node

   */
    var nodePropertyValueStringArrayConsumer = { node, propertyListTuple ->
        if (propertyListTuple._2() != null && propertyListTuple._2().size > 0) {
            val tx = this.graphDb.beginTx()
            try {
                node.setProperty(propertyListTuple._1(), propertyListTuple._2())
                tx.success()
            } catch (e: Exception) {
                tx.failure()
                AsyncLoggingService.logError(e.message)
            } finally {
                tx.close()
            }
        }
    }


    var readResourceFileFunction = { fileName ->
        try {
            val cl = javaClass.classLoader
            val uri = cl.getResource(fileName)!!.toURI()
            return Paths.get(uri)
        } catch (e: Exception) {
            println(e.message)
            e.printStackTrace()
        }

        null
    }

    fun shutDown() {
        AsyncLoggingService.logInfo("Shutting down database ...")
        graphDb.shutdown()
    }

    companion object {

        private val log = Logger.getLogger(FunctionLib::class.java)

        fun generateLineStreamFromPath(path: Path): Stream<String> {
            try {
                return Files.lines(path)
            } catch (e: IOException) {
                log.error(e.message)
                e.printStackTrace()
            }

            return Stream.empty()
        }

        @Deprecated("")
        var processTokensFunction = { tokens ->
            Arrays.stream<String>(tokens).map { token -> token.toUpperCase().trim { it <= ' ' } }
                    .collect<List<String>, Any>(Collectors.toList()).toTypedArray() as Array<String>
        }

        @JvmStatic
        fun main(args: Array<String>) {
            val path = FunctionLib(GraphDatabaseServiceSupplier.RunMode.READ_ONLY).readResourceFileFunction.apply("intact.txt")
            log.info("File exists = $path")
            log.info("There are " + FunctionLib.generateLineStreamFromPath(path).count()
                    + " lines in file " + path.toString())

        }
    }

}

