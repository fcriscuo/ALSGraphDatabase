package org.biodatagraphdb.alsdb.lib

/**
 * Created by fcriscuo on 3/18/20.
 */
interface Refined<in T> {
    abstract fun isValid(value: T) : Boolean
}