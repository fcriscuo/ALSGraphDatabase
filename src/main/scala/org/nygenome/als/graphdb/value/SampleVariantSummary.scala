package org.nygenome.als.graphdb.value

import org.apache.commons.csv.CSVRecord

case class SampleVariantSummary(
                               hugoGeneName:String, ensemblGeneId:String, sampleId:Int,
                               variantList:Array[String]
                              ) {}
object SampleVariantSummary extends ValueTrait{
  val columnHeadings:Array[String] = Array(
    "hugo_gene_name",
      "ensembl_gene_id",
      "sample_id",
      "num_variants",
      "num_any_impact_variants",
      "num_high_impact_variants",
      "num_modifier_impact_variants",
      "num_moderate_impact_variants",
      "num_low_impact_variants",
      "most_deleterious_variant_effect",
      "most_deleterious_variant_hgvs_change",
      "num_cnv_structural_variants",
      "num_high_impact_cnv",
      "num_moderate_impact_cnv",
      "num_non_cnv_structural_variants"
  )
  def parseCSVRecord(record: CSVRecord):SampleVariantSummary = {
    new SampleVariantSummary(
      record.get("hugo_gene_name"),
      record.get("ensembl_gene_id"),
      record.get("sample_id").toInt,
      record.get("most_deleterious_variant_hgvs_change").split(",")
    )
  }
}
