
package org.nygenome.als.graphdb;

import com.google.common.base.Stopwatch;
import com.google.common.base.Suppliers;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import java.util.function.Supplier;
import org.apache.log4j.Logger;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.nygenome.als.graphdb.consumer.IntactDataConsumer;
import org.nygenome.als.graphdb.consumer.PathwayInfoConsumer;
import org.nygenome.als.graphdb.consumer.UniProtValueConsumer;
import org.nygenome.als.graphdb.supplier.GraphDatabaseServiceSupplier;
import org.nygenome.als.graphdb.util.FrameworkPropertyService;


public enum EmbeddedGraph
{

	INSTANCE;
	private static final Logger log = Logger.getLogger(EmbeddedGraph.class);
	private  final Path DB_PATH = Paths.get(FrameworkPropertyService.INSTANCE.getStringProperty("neo4j.db.path"));
	private  GraphDatabaseService graphDb  = Suppliers.memoize(new GraphDatabaseServiceSupplier(DB_PATH)).get();
	private AlsNetwork protNet = new AlsNetwork();

	public static enum RelTypes implements RelationshipType {
		eNoEvent, IN_PATHWAY, BIOMARKER, THERAPEUTIC, GENETIC_VARIATION, KANEKO_ASSOCIATED,
		PPI_ASSOCIATION, PPI_COLOCALIZATION,
		PPI_GENETIC_INTERACTION, PPI_PREDICTED_INTERACTION, TISSUE_ENHANCED, DRUG_TARGET,
		DRUG_ENZYME, DRUG_TRANSPORTER, DRUG_CARRIER, PART_OF, DEG_RELATED_TO, SEQ_SIM,
		GO_CLASSIFICATION,  TRANSCRIPT, IMPLICATED_IN, HAS_SAMPLE, SAMPLED_FROM, MAPS_TO,
		EXPRESSION_LEVEL, EXPRESSED_PROTEIN, ENCODED_BY,ASSOCIATED_PROTEIN,ASSOCIATED_GENETIC_ENTITY
	}

	public static enum LabelTypes implements Label {
		Ensembl, HUGO, GeneOntology, Transcript, Pathway, Disease, Protein, Tissue,
    Drug, GEOStudy, GEOComparison, Gene, Subject, Sample,
    Expression,TPM,Xref,EnsemblGene, EnsemblTranscript,MolecularFunction,
		BiologicalProcess, CellularComponents,Unknown,Drug_Target, Drug_Enzyme, Drug_Transporter,
		Drug_Carrier, GeneticEntity
	}

	// convenience method to satisfy legacy usages
	public  static GraphDatabaseService getGraphInstance() {
		return EmbeddedGraph.INSTANCE.graphDb;
	}

	public static void main(final String[] args) {

		EmbeddedGraph.INSTANCE.createDb();
		EmbeddedGraph.INSTANCE.shutDown();
	}

	public Supplier<Transaction> transactionSupplier = () ->
			graphDb.beginTx();

	void createDb() {
		// START SNIPPET: transaction
		try (Transaction tx = graphDb.beginTx()) {
			try {
				Stopwatch stopwatch = Stopwatch.createStarted();
				System.out.println("read uniprot to ensembl mapping");
				FrameworkPropertyService.INSTANCE
						.getOptionalPathProperty("UNIPROT_ENSEMBL_TRANSCRIPT_ASSOCIATION_FILE")
						.ifPresent(new UniProtValueConsumer());
				System.out.println("readPathwayInfo");
        FrameworkPropertyService.INSTANCE
            .getOptionalPathProperty("UNIPROT_REACTOME_HOMOSAPIENS_MAPPING")
            .ifPresent(new PathwayInfoConsumer());
				// protein - protein interactions
        System.out.println("read protein-protein interaction file");
        FrameworkPropertyService.INSTANCE
            .getOptionalPathProperty("PPI_INTACT_DIR")
            .ifPresent(new IntactDataConsumer());
        System.out.println("read the Human Tissue Atlas data");
//				System.out.println("readHumanTissueAtlasInfo");
//				protNet.readHumanTissueAtlasInfo();
//				System.out.println("readDataFromDisGeNETFile");
//				protNet.readDataFromDisGeNETFile();
//				System.out.println("readDataFromKanekoPaper");
//				protNet.readDataFromKanekoPaper();
//				System.out.println("readDrugInfo");
//				protNet.readDrugInfo();
//				System.out.println("readGEOStudyInfo");
//				protNet.readGEOStudyInfo();
//				System.out.println("addSeqSimilarityInfo");
//				protNet.addSeqSimilarityInfo();

				stopwatch.stop();
				log.info("Time (in seconds) is : " + stopwatch.elapsed(TimeUnit.SECONDS));
			} catch (Exception e) {
				log.error(e.getMessage());
				e.printStackTrace();
			}
			tx.success();
		}

	}

	void shutDown() {
		System.out.println();
		System.out.println("Shutting down database ...");
		graphDb.shutdown();

	}



}
