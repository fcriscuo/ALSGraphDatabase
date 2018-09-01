package org.nygenome.als.graphdb.value

trait ValueTrait {
  private val HUMAN_SPECIES = "Homo sapiens"
  protected var parseStringOnPipeFunction: Function[String, List[String]] = (s: String) => {
    s.split("\\|").toList.map(_.trim).toList
  }
  protected var parseStringOnColonFunction: Function[String, List[String]] = (s: String) => {
    s.split(":").toList.map(_.trim).toList
  }
  protected var parseStringOnSemiColonFunction: Function[String, List[String]] = (s: String) => {
    s.split(";").toList.map(_.trim).toList
  }
protected def isHuman(species:String):Boolean = species.trim().equalsIgnoreCase(HUMAN_SPECIES)



}
