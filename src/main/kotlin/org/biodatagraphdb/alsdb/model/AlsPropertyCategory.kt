package org.biodatagraphdb.alsdb.model

import org.apache.commons.csv.CSVRecord

/**
 * Created by fcriscuo on 2/3/20.
 */

data class AlsPropertyCategory(
        val category:String, val parentCategory:String) {
    val id = category
    val isSelfReferential = category == parentCategory

    companion object : AlsdbModel {
        const val CATEGORY = "category"
        const val PARENT_CATEGORY = "parent_category"
        fun parseCSVRecord(record: CSVRecord): AlsPropertyCategory =
                AlsPropertyCategory(record.get(CATEGORY),
                    record.get(PARENT_CATEGORY))
    }
}