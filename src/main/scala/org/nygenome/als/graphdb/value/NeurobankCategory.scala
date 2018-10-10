package org.nygenome.als.graphdb.value

import org.apache.commons.csv.CSVRecord

case class NeurobankCategory (category:String, parentCategory:String ){
  val id:String = category
  val isSelfReferential:Boolean = category.equalsIgnoreCase(parentCategory)
}
object NeurobankCategory extends ValueTrait {

  def parseCSVRecord(record:CSVRecord):NeurobankCategory = {
    new NeurobankCategory(
      record.get("category"),
      record.get("parent_category")
    )
  }
}
