package edu.jhu.fcriscu1.als.graphdb.value

import com.datagraphice.fcriscuo.alsdb.graphdb.value.ValueTrait
import org.apache.commons.csv.CSVRecord

case class AlsAssociatedGene(
                            geneSymbol: String,
                            geneName: String
                            )  {
  val id:String = geneSymbol
}
object AlsAssociatedGene extends ValueTrait {

  def parseCSVRecord(record:CSVRecord):AlsAssociatedGene = {
    new AlsAssociatedGene(
      record.get("Gene symbol"),
      record.get("Gene name")
    )
  }
}
