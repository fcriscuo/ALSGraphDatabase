package org.biodatagraphdb.alsdb.model

import org.apache.commons.csv.CSVRecord

data class SampleVariantSummary (
       val hugoGeneName:String, val ensemblGeneId:String, val extSampleId:String,
        val numVariants:Int,
        val variantList:List<String>, val id:String
){
    companion object : AlsdbModel {
        val columnHeadings = arrayOf(
        "external_sample_id",
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
        "num_non_cnv_structural_variants")

        fun parseCSVRecord(record: CSVRecord):SampleVariantSummary =
            SampleVariantSummary(
                    record.get("hugo_gene_name"),
            record.get("ensembl_gene_id"),
            record.get("external_sample_id"),
            parseValidIntegerFromString(record.get("num_variants")),
            record.get("most_deleterious_variant_hgvs_change").split(","),
            "${record.get("external_sample_id")}:${record.get("ensembl_gene_id")}"
            )
    }
}