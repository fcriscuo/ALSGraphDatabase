
package org.nygenome.als.graphdb;

import com.google.common.base.Stopwatch;
import com.google.common.base.Suppliers;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
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
		eNoEvent, IN_PATHWAY, BIOMARKER, THERAPEUTIC, GENETIC_VARIATION, KANEKO_ASSOCIATED, PPI_ASSOCIATION, PPI_COLOCALIZATION,
		PPI_GENETIC_INTERACTION, PPI_PREDICTED_INTERACTION, TISSUE_ENHANCED, DRUG_TARGET,
		DRUG_ENZYME, DRUG_TRANSPORTER, DRUG_CARRIER, PART_OF, DEG_RELATED_TO, SEQ_SIM
	}

	public static enum LabelTypes implements Label {
		Pathway, Disease, Protein, Tissue, Drug, GEOStudy, GEOComparison, Gene
	}

	// convenience method to satisfy legacy usages
	public  static GraphDatabaseService getGraphInstance() {
		return EmbeddedGraph.INSTANCE.graphDb;
	}

	public static void main(final String[] args) {

		EmbeddedGraph.INSTANCE.createDb();
		EmbeddedGraph.INSTANCE.shutDown();
	}

	void createDb() {
		// START SNIPPET: transaction
		try (Transaction tx = graphDb.beginTx()) {
			try {
				Stopwatch stopwatch = Stopwatch.createStarted();
				System.out.println("readPathwayInfo");
				protNet.readPathwayInfo();
			//	System.out.println("readDataFromIntact");
    		//	protNet.readDataFromIntact();
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
