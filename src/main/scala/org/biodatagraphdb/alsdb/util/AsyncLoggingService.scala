package org.biodatagraphdb.alsdb.util

import com.twitter.logging._
import com.twitter.util.Local

object AsyncLoggingService {

  val fileHandler = FileHandler(
    filename =  "/tmp/logs/alsdatabase_" +java.time.LocalDate.now() +".log",
    rollPolicy = Policy.Daily,
    append = true,
    formatter = BareFormatter,
    level = Some(Logger.DEBUG)
  ).apply()
  val local = new Local[String]
  val formatter = new Formatter {
    def format(record: LogRecord) =
      local().getOrElse("MISSING!") + ":" + formatText(record) + lineTerminator
  }
  val consoleHandler = new ConsoleHandler(formatter, Some(Logger.DEBUG))
  val queueHandler = new QueueingHandler(fileHandler)
  val factory = LoggerFactory(
    node ="",
    level =  Some(Level.DEBUG)
  )
  val logger = factory()
  logger.addHandler(queueHandler)
  logger.addHandler(consoleHandler)

  def logError (msg:String):Unit = {
    logger.error(msg)
  }
  def logInfo (msg:String):Unit = {
    logger.info(msg)
  }
  def logDebug (msg:String):Unit = {
    logger.debug(msg)
  }

  def close():Unit = {
    queueHandler.flush()
    queueHandler.close()
  }


}
