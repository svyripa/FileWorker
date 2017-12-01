import java.io.PrintWriter

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer

import scala.concurrent.Future
import scala.io.Source
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global

object UniqBigFile extends App {
  val start = System.currentTimeMillis()

  // In memory
  val bifFileIter = Source.fromFile("bigToUniq.csv", "UTF-8")
  val bigFile = bifFileIter.getLines().toList.groupBy(identity)

  val pw = new PrintWriter("uniqed-big-file.csv", "UTF-8")

  import akka.stream.scaladsl.Source

  implicit val sys = ActorSystem("actor-system")
  implicit val materializer = ActorMaterializer()

  Source.fromIterator(() => bigFile.iterator)
    .map(_._2.head)
    .runForeach(pw.println)
    .andThen {
      case _try => {
        pw.close()
        bifFileIter.close()
        _try match {
          case Success(s) =>
            println(s"Done successfully. Time: ${(System.currentTimeMillis().toDouble - start) / 1000}")
          case Failure(f) =>
            println(s"Failed with error: $f")
        }
      }
    }
}
