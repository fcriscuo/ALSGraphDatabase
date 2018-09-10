package org.nygenome.als.graphdb.value

import java.util
import java.util.List

abstract class AbstractUniProtDrug extends ValueTrait{
  def drugModelType: String 
  def id: String 
  def name: String 
  def geneName: String 
  def genbankProteinId: String 
  def genbankGeneId: String 
  def uniprotId: String 
  def uniprotTitle: String 
  def pdbId: String 
  def geneCardId: String 
  def geneAtlasId: String 
  def hgncId: String 
  def species: String 
  def drugIdList: util.List[String] 

}
