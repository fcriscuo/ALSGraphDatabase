package org.biodatagraphdb.alsdb.function

import com.twitter.concurrent.{Broker, Offer}

object SievePOC {

  def integers(from: Int): Offer[Int] = {
    val b = new Broker[Int]
    def gen(n: Int): Unit = b.send(n).sync() ensure gen(n + 1)
    gen(from)
    b.recv
  }


}
