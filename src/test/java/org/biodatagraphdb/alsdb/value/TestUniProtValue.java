package org.biodatagraphdb.alsdb.value;

import org.biodatagraphdb.alsdb.model.UniProtValue;
import org.biodatagraphdb.alsdb.util.TsvRecordStreamSupplier;
import static org.biodatagraphdb.alsdb.lib.StringFunctionsKt.displayGeneOntologyList;

public class TestUniProtValue {

    public static void main(String[] args) {
        try {
            new TsvRecordStreamSupplier(org.biodatagraphdb.alsdb.model.UniProtValue.Companion.getDefaultFilePath())
                    .get()
                    .limit(100)
                    .map(UniProtValue.Companion::parseCSVRecord)
                    .forEach(upv -> {
                        System.out.println(">>>>>>>" + upv.getUniprotId());
                        displayGeneOntologyList("Gene Ontology (cellular component)", upv.getGoCellComponentList());
                        displayGeneOntologyList("Gene Ontology (bio process)", upv.getGoBioProcessList());
                        displayGeneOntologyList("Gene Ontology (mol function)", upv.getGoMolFuncList());
                        System.out.println(" ");
                    });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
