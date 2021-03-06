package com.datagraphice.fcriscuo.alsdb.graphdb.value;


import java.util.function.BiConsumer;
import com.datagraphice.fcriscuo.alsdb.graphdb.util.TsvRecordStreamSupplier;
import edu.jhu.fcriscu1.als.graphdb.util.StringUtils;
import scala.Tuple2;
import scala.collection.immutable.List;


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
      new TsvRecordStreamSupplier(edu.jhu.fcriscu1.als.graphdb.value.UniProtValue.defaultFilePath()).get()
          .limit(100)
          .map(edu.jhu.fcriscu1.als.graphdb.value.UniProtValue::parseCSVRecord)
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
