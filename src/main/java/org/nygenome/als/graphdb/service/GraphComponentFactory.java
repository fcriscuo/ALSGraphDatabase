package org.nygenome.als.graphdb.service;

import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.eclipse.collections.impl.factory.Maps;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.nygenome.als.graphdb.app.ALSDatabaseImportApp;
import org.nygenome.als.graphdb.app.ALSDatabaseImportApp.LabelTypes;
import org.nygenome.als.graphdb.lib.FunctionLib;
import org.nygenome.als.graphdb.value.RnaTpmGene;
import org.nygenome.als.graphdb.util.AsyncLoggingService;
import org.nygenome.als.graphdb.value.GeneOntology;
import scala.Tuple2;

/**
 * A Singleton service class implemented as an enum responsible for maintaining Maps of existing
 * Nodes and Relationships and for creating new ones This is necessary so that particular Nodes and
 * Relationships can be created the first time they occur in one of the data source file. It also
 * allows some flexibility in the order of how source data is loaded as well as the option of
 * supporting concurrent source data loading
 *
 * @author fcriscuolo
 */
public enum GraphComponentFactory {
  INSTANCE;

  private FunctionLib lib = new FunctionLib();
  private final Supplier<Node> unknownNodeSupplier = () -> {
    try (Transaction tx = ALSDatabaseImportApp.INSTANCE.transactionSupplier.get()) {
      return ALSDatabaseImportApp.getGraphInstance().createNode(LabelTypes.Unknown);
    }
  };


  private Map<String, Node> proteinMap = Maps.mutable.empty();
  private Map<String, Node> geneOntologyMap = Maps.mutable.empty();
  private Map<String, Node> xrefMap = Maps.mutable.empty();
  private Map<String, Node> geneticEntityMap = Maps.mutable.empty();
  private Map<String, Node> rnaTpmGeneMap = Maps.mutable.empty();
  private Map<String, Node> diseaseMap = Maps.mutable.empty();
  private Map<String, Node> drugMap = Maps.mutable.empty();
  private Map<String, Node> pathwayMap = Maps.mutable.empty();
  private Map<String, Node> tissueMap = Maps.mutable.empty();
  private Map<String, Node> subjectMap = Maps.mutable.empty();
  private Map<String, Node> sampleMap = Maps.mutable.empty();
  private Map<String, Node> snpMap = Maps.mutable.empty();

  /*
  SNP Node
   */

  private Function<String, Node> createSnpNodeFunction = (snpId) -> {
    Transaction tx = ALSDatabaseImportApp.INSTANCE.transactionSupplier.get();
    try {
      Node snpNode = ALSDatabaseImportApp.getGraphInstance()
          .createNode(LabelTypes.Variant);
      snpNode.addLabel(LabelTypes.SNP);
      lib.nodePropertyValueConsumer
          .accept(snpNode, new Tuple2<>("snpId", snpId));
      snpMap.put(snpId, snpNode);
      tx.success();
      return snpNode;
    } catch (Exception e) {
      tx.failure();
      e.printStackTrace();
    } finally {
      tx.close();
    }
    return unknownNodeSupplier.get();
  };

  private Function<String,Node> createSampleVariantNodeFunction = (svId) -> {
    Transaction tx = ALSDatabaseImportApp.INSTANCE.transactionSupplier.get();
    try {
      Node sampleVariantNode = ALSDatabaseImportApp.getGraphInstance()
          .createNode(LabelTypes.SampleVariant);
      lib.nodePropertyValueConsumer.accept(sampleVariantNode, new Tuple2<>("ID",svId));
      tx.success();
      return sampleVariantNode;
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      tx.close();
    }
    return unknownNodeSupplier.get();
  };

  public Function<String,Node> getSampleVariantNodeFunction = (id) ->
      createSampleVariantNodeFunction.apply(id);

  public Function<String, Node> getSnpNodeFunction = (snpId) ->
      (snpMap.containsKey(snpId)) ? snpMap.get(snpId)
          : createSnpNodeFunction.apply(snpId);
  /*
  Sample Node
   */
  private Function<String, Node> createSampleNodeFunction = (extSampleId) ->
  {
    Transaction tx = ALSDatabaseImportApp.INSTANCE.transactionSupplier.get();
    try {
      Node sampleNode = ALSDatabaseImportApp.getGraphInstance()
          .createNode(LabelTypes.Sample);
      lib.nodePropertyValueConsumer
          .accept(sampleNode, new Tuple2<>("ExternalSampleId", extSampleId));
      sampleMap.put(extSampleId, sampleNode);
      tx.success();
      return sampleNode;
    } catch (Exception e) {
      e.printStackTrace();
      tx.failure();
    } finally {
      tx.close();
    }
    return unknownNodeSupplier.get();
  };

  public Function<String, Node> getSampleNodeFunction = (extSampleId) ->
      (sampleMap.containsKey(extSampleId)) ? sampleMap.get(extSampleId)
          : createSampleNodeFunction.apply(extSampleId);
  /*
  Subject Node
   */

  private Function<String, Node> createSubjectNodeFunction = (extSubjectId) -> {
    Transaction tx = ALSDatabaseImportApp.INSTANCE.transactionSupplier.get();
    try {
      Node subjectNode = ALSDatabaseImportApp.getGraphInstance()
          .createNode(LabelTypes.Subject);
      lib.nodePropertyValueConsumer
          .accept(subjectNode, new Tuple2<>("ExternalSubjectId", extSubjectId));
      subjectMap.put(extSubjectId, subjectNode);
      tx.success();
      return subjectNode;
    } catch (Exception e) {
      tx.failure();
      e.printStackTrace();
    } finally {
      tx.close();
    }
    return unknownNodeSupplier.get();
  };

  public Function<String, Node> getSubjectNodeFunction = (extSubjectId) ->
      (subjectMap.containsKey(extSubjectId)) ? subjectMap.get(extSubjectId)
          : createSubjectNodeFunction.apply(extSubjectId);

  /*
  Human Tissue Node

   */
  private Function<String, Node> createHumanTissueNodeFunction = (id) -> {
    Transaction tx = ALSDatabaseImportApp.INSTANCE.transactionSupplier.get();
    try {
      Node tissueNode = ALSDatabaseImportApp.getGraphInstance()
          .createNode(LabelTypes.Tissue);
      lib.nodePropertyValueConsumer.accept(tissueNode, new Tuple2<>("ID", id));
      tissueMap.put(id, tissueNode);
      tx.success();
      AsyncLoggingService.logInfo("createHumanTissueNode for tissue ID: " + id);
      return tissueNode;
    } catch (Exception e) {
      tx.failure();
      e.printStackTrace();
    } finally {
      tx.close();
    }
    return unknownNodeSupplier.get();
  };

  public Function<String, Node> getHumanTissueNodeFunction = (id) ->
      (tissueMap.containsKey(id)) ? tissueMap.get(id)
          : createHumanTissueNodeFunction.apply(id);

  /*
  Pathway Node
   */
  private Function<String, Node> createPathwayNodeFunction = (pathwayId) -> {
    Transaction tx = ALSDatabaseImportApp.INSTANCE.transactionSupplier.get();
    try {
      Node node = ALSDatabaseImportApp.getGraphInstance()
          .createNode(LabelTypes.Pathway);
      pathwayMap.put(pathwayId, node);
      lib.nodePropertyValueConsumer.accept(node, new Tuple2<>("ReactomeId", pathwayId));
      AsyncLoggingService.logInfo("createPathway Node for Reactome ID: " + pathwayId);
      tx.success();
      return node;
    } catch (Exception e) {
      AsyncLoggingService.logError("ERR: createPathwayNodeFunction " + e.getMessage());
      tx.failure();
    } finally {
      tx.close();
    }
    return unknownNodeSupplier.get();
  };

  public Function<String, Node> getPathwayNodeFunction = (pathwayId) ->
      (pathwayMap.containsKey(pathwayId)) ? pathwayMap.get(pathwayId)
          : createPathwayNodeFunction.apply(pathwayId);

  /*
  Create a Disease node
   */
  private Function<String, Node> createDiseaseNodeFunction = (diseaseId) -> {
    Transaction tx = ALSDatabaseImportApp.INSTANCE.transactionSupplier.get();
    try {
      Node diseaseNode = ALSDatabaseImportApp.getGraphInstance()
          .createNode(LabelTypes.Disease);
      lib.nodePropertyValueConsumer.accept(diseaseNode, new Tuple2<>("DiseaseId", diseaseId));
      diseaseMap.put(diseaseId, diseaseNode);
      tx.success();
      AsyncLoggingService.logInfo("createDiseasekNode invoked for Disease id  " +
          diseaseId);
      return diseaseNode;
    } catch (Exception e) {
      e.printStackTrace();
      tx.failure();
    } finally {
      tx.close();
    }
    return unknownNodeSupplier.get();
  };

  public Function<String, Node> getDiseaseNodeFunction = (diseaseId) ->
      (diseaseMap.containsKey(diseaseId)) ? diseaseMap.get(diseaseId)
          : createDiseaseNodeFunction.apply(diseaseId);

  /*
  Create an RnaTpmGene node
   */
  private Function<RnaTpmGene, Node> createRnaTpmGeneNodeFunction = (rna) -> {
    Transaction tx = ALSDatabaseImportApp.INSTANCE.transactionSupplier.get();
    String id = rna.id();
    try {
      Node node = ALSDatabaseImportApp.getGraphInstance()
          .createNode(LabelTypes.Expression);
      node.addLabel(LabelTypes.TPM);

      lib.nodePropertyValueConsumer.accept(node, new Tuple2<>("SampleGeneId", id));
      // persist tpm value as a String
      lib.nodePropertyValueConsumer
          .accept(node, new Tuple2<>("TPM", String.valueOf(rna.tpm())));
      rnaTpmGeneMap.put(id, node);
      tx.success();
      AsyncLoggingService.logInfo("Created RnaTpmNode for id  " +
          id);
      return node;
    } catch (Exception e) {
      tx.failure();
      AsyncLoggingService.logError("ERR: RnaTpmNode for id  " +
          id + ": " + e.getMessage());
      e.printStackTrace();
    } finally {
      tx.close();
    }
    return unknownNodeSupplier.get();
  };
  public Function<RnaTpmGene, Node> getRnaTpmGeneNodeFunction = (rna) ->
      (rnaTpmGeneMap.containsKey(rna.id())) ? rnaTpmGeneMap.get(rna.id())
          : createRnaTpmGeneNodeFunction.apply(rna);

  /*
  Create a GeneOntology Node and set its properties
   */
  private Function<GeneOntology, Node> createGeneOntologyNodeFunction = (go) -> {
    Transaction tx = ALSDatabaseImportApp.INSTANCE.transactionSupplier.get();
    try {
      Node node = ALSDatabaseImportApp.getGraphInstance()
          .createNode(LabelTypes.GeneOntology);
      node.addLabel(lib.resolveGeneOntologyPrincipleFunction.apply(go.goAspect()));
      lib.nodePropertyValueConsumer.accept(node, new Tuple2<>("GeneOntologyId", go.goId()));
      lib.nodePropertyValueConsumer.accept(node, new Tuple2<>("GeneOntologyPrinciple",
          go.goAspect()));
      lib.nodePropertyValueConsumer.accept(node, new Tuple2<>("GeneOntologyName", go.goName()));
      geneOntologyMap.put(go.goId(), node);
      tx.success();
      return node;
    } catch (Exception e) {
      tx.failure();
      e.printStackTrace();
    } finally {
      tx.close();
    }
    return unknownNodeSupplier.get();
  };

  /*
  Public method to find or create a GeneOntology Node
   */
  public Function<GeneOntology, Node> getGeneOntologyNodeFunction = (go) ->
      (geneOntologyMap.containsKey(go.goId())) ? geneOntologyMap.get(go.goId())
          : createGeneOntologyNodeFunction.apply(go);

  private BiConsumer<String, Node> completeDrugBankNodeProperties = (id, node) -> {
    DrugBankService.INSTANCE.getDrugBankValueById(id)
        .ifPresent(dbv -> {
          lib.nodePropertyValueConsumer.accept(node, new Tuple2<>("DrugId", dbv.drugBankId()));
          lib.nodePropertyValueConsumer.accept(node, new Tuple2<>("DrugName", dbv.drugName()));
          lib.nodePropertyValueConsumer.accept(node, new Tuple2<>("DrugType", dbv.drugType()));
          lib.nodePropertyValueConsumer.accept(node, new Tuple2<>("CASNumber", dbv.casNumber()));
          lib.nodePropertyValueConsumer.accept(node, new Tuple2<>("RxListLink", dbv.rxListLink()));
          lib.nodePropertyValueConsumer.accept(node, new Tuple2<>("NDCLink", dbv.ndcLink()));
        });
  };

  /*
  Private Function to create a new DrugBank node
   */
  private Function<String, Node> createDrugBankNodeFunction = (dbId) -> {
    Transaction tx = ALSDatabaseImportApp.INSTANCE.transactionSupplier.get();
    AsyncLoggingService.logInfo("createDrugBankNode invoked for DrunkBank id  " +
        dbId);
    try {
      Node node = ALSDatabaseImportApp.getGraphInstance()
          .createNode(LabelTypes.Drug);
      lib.nodePropertyValueConsumer.accept(node, new Tuple2<>("DrugBankId",
          dbId));
      completeDrugBankNodeProperties.accept(dbId, node);
      drugMap.put(dbId, node);
      tx.success();
      return node;
    } catch (Exception e) {
      tx.failure();
      AsyncLoggingService.logError("ERR: createDrugBankNode: " +
          e.getMessage());
      e.printStackTrace();

    } finally {
      tx.close();
    }
    return unknownNodeSupplier.get();
  };

  public Function<String, Node> getDrugBankNodeFunction = (drugBankId) ->
      (drugMap.containsKey(drugBankId)) ? drugMap.get(drugBankId)
          : createDrugBankNodeFunction.apply(drugBankId);
  /*
  Private Function to create a new Protein Node
   */
  private Function<String, Node> createProteinNodeFunction = (uniprotId) -> {
    Transaction tx = ALSDatabaseImportApp.INSTANCE.transactionSupplier.get();
    AsyncLoggingService.logInfo("createProteinNodeFunction invoked for uniprot protein id  " +
        uniprotId);
    try {
      Node node = ALSDatabaseImportApp.getGraphInstance()
          .createNode(LabelTypes.Protein);
      lib.nodePropertyValueConsumer.accept(node, new Tuple2<>("UniProtId", uniprotId));
      proteinMap.put(uniprotId, node);
      tx.success();
      return node;
    } catch (Exception e) {
      e.printStackTrace();
      tx.failure();
    } finally {
      tx.close();
    }
    return unknownNodeSupplier.get();
  };

  public Function<String, Node> getProteinNodeFunction = (uniprotId) ->
      (proteinMap.containsKey(uniprotId)) ? proteinMap.get(uniprotId)
          : createProteinNodeFunction.apply(uniprotId);

  /*
  Private Function to create a new Xref Node
   */
  private Function<Tuple2<String, LabelTypes>, Node> createXrefNodeFunction = (xrefTuple) -> {
    Transaction tx = ALSDatabaseImportApp.INSTANCE.transactionSupplier.get();
    String xrefId = xrefTuple._1();
    LabelTypes type = xrefTuple._2();
    try {
      AsyncLoggingService.logInfo("createXrefNodeFunction invoked for XREF id  " +
          xrefId + " type " + type.toString());
      Node node = ALSDatabaseImportApp.getGraphInstance().createNode(LabelTypes.Xref);
      // add a type (e.g. ensembl, pubmed) label
      node.addLabel(type);
      lib.nodePropertyValueConsumer.accept(node, new Tuple2<>("Xref Id", xrefId));
      xrefMap.put(xrefId, node);
      tx.success();
      return node;
    } catch (Exception e) {
      tx.failure();
      AsyncLoggingService.logError("ERR: createXrefNodeFunction failed:  " +
          e.getMessage());
      e.printStackTrace();
    } finally {
      tx.close();
    }
    return unknownNodeSupplier.get();

  };

  public Function<Tuple2<String, LabelTypes>, Node> getXrefNodeFunction = (xrefTuple) ->
      (xrefMap.containsKey(xrefTuple._1()) ? xrefMap.get(xrefTuple._1())
          : createXrefNodeFunction.apply(xrefTuple));

  private Function<String,Optional<LabelTypes>> resolveGeneticEntityLabelFunction = (id) -> {
    if (id.toUpperCase().startsWith("ENSG")) {
      return Optional.of(LabelTypes.EnsemblGene);
    }
    if (id.toUpperCase().startsWith("ENST")) {
      return Optional.of(LabelTypes.EnsemblTranscript);
    }
    return Optional.empty();
  };


  /*
  Private Function that creates a new GeneticEntity node for a specified HUGO Gene Symbol
  A second label identifies the genetic entity as a Gene
   */
  private Function<String, Node> createGeneNodeFunction = (geneticEntityId) -> {
    Transaction tx = ALSDatabaseImportApp.INSTANCE.transactionSupplier.get();
    try {
      AsyncLoggingService.logInfo("createGeneticEntityNodeFunction invoked for id:  " +
          geneticEntityId);
      Node node = ALSDatabaseImportApp.getGraphInstance().createNode(LabelTypes.GeneticEntity);
      resolveGeneticEntityLabelFunction.apply(geneticEntityId).ifPresent(node::addLabel);
      geneticEntityMap.put(geneticEntityId, node);
      tx.success();
      return node;
    } catch (Exception e) {
      tx.failure();
      AsyncLoggingService.logError("ERR: createGeneticEntityNodeFunction failed:  " +
          e.getMessage());
      e.printStackTrace();
    } finally {
      tx.close();
    }
    return unknownNodeSupplier.get();
  };
  /*
  Protected Function that resolves a Gene by either finding an existing Node
  with a specified gene symbol or creating a new Node for that symbol
   */
public Function<String, Node> getGeneticEntityNodeFunction=(geneticEntityId)->
    (geneticEntityMap.containsKey(geneticEntityId))?geneticEntityMap.get(geneticEntityId)
    :createGeneNodeFunction.apply(geneticEntityId);
    }
