import java.io.PrintWriter

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.matching.Regex
import scala.util.{Failure, Success}

object LemmasFromTitles /*extends App*/ {
  val t1 = System.currentTimeMillis()

  implicit val system = ActorSystem("my-actor-system")
  implicit val materializer = ActorMaterializer()

  val dir = "/home/heroys6/Desktop/BK-607/Inp/"
  val iterator: Iterator[String] = scala.io.Source
    .fromFile(dir + "test.csv", "utf-8") // titles_all_uniq.tsv
    .getLines()
  val pattern: Regex = """[а-яА-Яa-zA-Z]+""".r
  val writer = new PrintWriter(dir + "test-res.csv") //

  Source.fromIterator(() => iterator)
    .mapAsyncUnordered(32) { row =>
      Future {
        val words = pattern.findAllIn(row).toList
        val normalized =
          if (words.head.charAt(0).isUpper && words.head.length > 1 && words.head.charAt(1).isLower)
            words.head.toLowerCase :: words.tail
          else
            words
        //println("Normalized: " + normalized)
        normalized.filter(_.charAt(0).isUpper)
      }
    }
    .flatMapConcat {
      case x => Source.fromIterator(() => x.iterator)
    }
    .filterNot(_ == "BOOKIMED")
    .filterNot(_ == "Bookimed")
    .filter(_.length > 3)
    .runForeach(writer.println)
    .andThen { case _try =>
      writer.close()

      _try match {
        case Success(_) =>
          println(s"Success, time: ${(System.currentTimeMillis() - t1).toFloat / 1000} sec")
        case Failure(f) =>
          println(s"Failure, time: ${(System.currentTimeMillis() - t1).toFloat / 1000} sec: " + f)
      }
    }
}
