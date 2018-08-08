package org.nygenome.als.graphdb.consumer;

import com.twitter.util.Duration;
import com.twitter.util.Stopwatches;
import java.util.function.Consumer;
import org.apache.commons.csv.CSVRecord;

import lombok.extern.log4j.Log4j;
import org.nygenome.als.graphdb.EmbeddedGraph;
import org.nygenome.als.graphdb.util.CsvRecordStreamSupplier;
import org.nygenome.als.graphdb.util.FrameworkPropertyService;
import org.nygenome.als.graphdb.util.TsvRecordStreamSupplier;
import scala.Tuple2;
import javax.annotation.Nonnull;
import java.nio.file.Path;
import java.util.function.Predicate;

/*
Represents a subclass of GraphDataConsumer specific for importing
UniProt -> Reactome associations
Source  tsv file columns: UniProt_ID
                          Reactome_ID
                          URL
                          Event_Name
                          Evidence_Code
                          Species

 */
@Log4j
public class PathwayInfoConsumer extends GraphDataConsumer implements Consumer<Path>{

  private void createPathwayNode(String szPathwayId, String szPathwayName) {
    pathwayMap.put(szPathwayId, EmbeddedGraph.getGraphInstance()
        .createNode(EmbeddedGraph.LabelTypes.Pathway));
    pathwayMap.get(szPathwayId).setProperty("PathwayName", szPathwayName);
    log.info("Createad Pathway Node: " +szPathwayName);
  }
/*
Private Consumer that will persist the pathway data into the graph database
 */
private Consumer<PathwayRecord> pathwayRecordConsumer = (record) ->{
   if (!vPathwayMap.containsKey(record.getUniprotReactomeIdTuple())) {
       if(!pathwayMap.containsKey(record.getUniprotId())){
           // create a protein node
           createProteinNode(strNoInfo, record.getUniprotId(), strNoInfo,
               strNoInfo, strNoInfo, strNoInfo);
       }
       if(!pathwayMap.containsKey(record.getReactomeId())) {
           createPathwayNode(record.getReactomeId(), record.getEventName());
       }

       vPathwayMap.put(
           record.getUniprotReactomeIdTuple(),
           proteintMap.get(record.getUniprotId()).createRelationshipTo(
               pathwayMap.get(record.getReactomeId()),
               EmbeddedGraph.RelTypes.IN_PATHWAY));
       vPathwayMap.get(record.getUniprotReactomeIdTuple()).setProperty("Reference",
           "Reactome");
   }

};

/*
Public Consumer get method to process the UniProtReactome tsv file at a
specified path
 */

    @Override
    public void accept(@Nonnull Path path) {
        Duration duration = Stopwatches.start().apply(); // start a stopwatch
        new TsvRecordStreamSupplier(path).get().map(PathwayRecord::new)
                .filter(homoSapiensPredicate)// only human entries
                .forEach(pathwayRecordConsumer);
        log.info("Processing pathway file " +path.toString()
            +" required " +duration.inSeconds() +" seconds" );

    }

    public class PathwayRecord {
        String uniprotId;
        String reactomeId;
        String url;
        String eventName;
        String evidenceCode;
        String species;


     PathwayRecord (@Nonnull CSVRecord record){
            this.uniprotId = record.get("UniProt_ID");
            this.reactomeId = record.get("Reactome_ID");
            this.url = record.get("URL");
            this.eventName = record.get("Event_Name");
            this.evidenceCode = record.get("Evidence_Code");
            this.species = record.get("Species");
        }

        @Override
        public String toString() {
            return "PathwayRecord{" +
                    "uniprotId='" + uniprotId + '\'' +
                    ", reactomeId='" + reactomeId + '\'' +
                    ", url='" + url + '\'' +
                    ", eventName='" + eventName + '\'' +
                    ", evidenceCode='" + evidenceCode + '\'' +
                    ", species='" + species + '\'' +
                    '}';
        }

        public  Tuple2<String,String> getUniprotReactomeIdTuple() {
         return  new Tuple2<>(this.uniprotId, this.reactomeId);
        }

        public String getUniprotId() {
            return uniprotId;
        }

        public String getReactomeId() {
            return reactomeId;
        }

        public String getUrl() {
            return url;
        }

        public String getEventName() {
            return eventName;
        }

        public String getEvidenceCode() {
            return evidenceCode;
        }

        public String getSpecies() {
            return species;
        }



    }

    public static void main(String... args) {
        FrameworkPropertyService.INSTANCE.getOptionalPathProperty("UNIPROT_REACTOME_HOMOSAPIENS_MAPPING")
                .ifPresent(new PathwayInfoConsumer());
    }

}
