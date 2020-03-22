package org.biodatagraphdb.alsdb.model

/**
 * Created by fcriscuo on 3/12/20.
 * Interface to support value type classes
 */
interface Refined <in T> {
    abstract fun isValid(value: T) : Boolean
}