package org.biodatagraphdb.alsdb.model

import io.kotlintest.matchers.collections.shouldHaveElementAt
import io.kotlintest.matchers.collections.shouldHaveSize
import io.kotlintest.specs.WordSpec

class AlsdbModelTest: AlsdbModel, WordSpec() {
    init {
        val testSemicolonString = "PartA;PartB;PartC;PartD"
        "Parse by semi colon function" should {
            "should create a list with" {
                parseStringOnSemiColon(testSemicolonString).shouldHaveSize(4)
            }
            "have PartC"{
                parseStringOnSemiColon(testSemicolonString).shouldHaveElementAt(2,"PartC")
            }
        }
        val testColonString = "Item1:Item2:Item3:Item4:Item5"
        "Parse by colon function" should {
            "should creat a list with" {
                parseStringOnColon(testColonString).shouldHaveSize(5)
            }
            "have Item2"{
                parseStringOnColon(testColonString).shouldHaveElementAt(1,"Item2")
            }
        }

    }
}