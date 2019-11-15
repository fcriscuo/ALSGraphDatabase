package org.biodatagraphdb.alsdb

import com.twitter.app.App

object AppFlagsPoc extends App {
  val n = flag("n", 100, "Number of items to process")
  def main():Unit = {
    print("Value supplied for n = " +n)
  }
}
