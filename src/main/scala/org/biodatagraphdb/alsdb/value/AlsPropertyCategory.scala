package org.biodatagraphdb.alsdb.value

import org.apache.commons.csv.CSVRecord

case class AlsPropertyCategory(category:String, parentCategory:String ){
  val id:String = category
  val isSelfReferential:Boolean = category.equalsIgnoreCase(parentCategory)
}
object AlsPropertyCategory extends ValueTrait {

  def parseCSVRecord(record:CSVRecord):AlsPropertyCategory = {
    new AlsPropertyCategory(
      record.get("category"),
      record.get("parent_category")
    )
  }
}
