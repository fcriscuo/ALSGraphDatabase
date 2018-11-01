package org.nygenome.als.graphdb.value

import org.apache.commons.csv.CSVRecord

case class UniProtBlastResult(
      sourceUniprotId:String,
      hitUniprotId:String,
      score:Double,
      eValue:String // no direct way to store exponental data
                             ) {

}

object UniProtBlastResult extends ValueTrait {
val columnHeadings:Array[String] = Array("sourceNumber","sourceId", "sourceLength","sourcePercentIdentity",
  "hitId", "hitLength", "hitPercentIdentity", "score", "eValue", "x")

  def parseCSVRecord(record:CSVRecord):UniProtBlastResult = {
    new UniProtBlastResult(
      parseStringOnPipeFunction(record.get("sourceId"))(1),
      record.get("hitId"),  record.get("score").toDouble,
      record.get("eValue")
      )

  }
}