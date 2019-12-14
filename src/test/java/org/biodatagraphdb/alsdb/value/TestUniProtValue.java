package org.biodatagraphdb.alsdb.value;

import java.util.function.BiConsumer;
import org.biodatagraphdb.alsdb.util.TsvRecordStreamSupplier;
import scala.Tuple2;
import scala.collection.immutable.List;
import org.biodatagraphdb.alsdb.util.StringUtils;

public class TestUniProtValue {

  static BiConsumer<String, List<String>> displayStringConsumer = (title,list) ->{
     scala.collection.Iterator iter = list.iterator();
     while (iter.hasNext()) {
       String entry = (String) iter.next();
       Tuple2<String,String> entryTuple = StringUtils.parseGeneOntologyEntry(entry);
       System.out.println(title + "  " +entryTuple._1() +"  " +entryTuple._2());
     }
     };

  public static void main(String[] args) {
    try {
      new TsvRecordStreamSupplier(org.biodatagraphdb.alsdb.value.UniProtValue.defaultFilePath()).get()
          .limit(100)
          .map(org.biodatagraphdb.alsdb.value.UniProtValue::parseCSVRecord)
          .forEach(upv -> {
            System.out.println(">>>>>>>" +upv.uniprotId());
            displayStringConsumer.accept("Gene Ontology (cellular component)", upv.goCellComponentList());
            displayStringConsumer.accept("Gene Ontology (bio process)", upv.goBioProcessList());
            displayStringConsumer.accept("Gene Ontology (mol function)", upv.goMolFuncList());
            System.out.println(" ");
          });
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
