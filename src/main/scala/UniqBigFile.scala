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
//  val bigFileIter = Source.fromFile("bigToUniq.csv", "UTF-8")
  val bigFileIter = Source.fromFile("GSC_Bukv_PP2_2.csv", "UTF-8")
  val bigFile = bigFileIter.getLines().grouped(65536)

  val pw = new PrintWriter("uniqed-big-file.csv", "UTF-8")

  import akka.stream.scaladsl.Source

  implicit val sys = ActorSystem("actor-system")
  implicit val materializer = ActorMaterializer()

  Source.fromIterator(() => bigFile)
    .mapAsyncUnordered(4)(batch => Future(batch.groupBy(identity)))
    .foldAsync(Map.empty[String, Seq[String]])((l, r) => Future(l ++ r))
    .fold(Map.empty[String, Seq[String]])(_ ++ _)
    .runForeach(m => m.foreach(x => pw.println(x._1)))
    .andThen {
      case _try => {
        pw.close()
        bigFileIter.close()
        _try match {
          case Success(s) =>
            println(s"Done successfully. Time: ${(System.currentTimeMillis().toDouble - start) / 1000}")
          case Failure(f) =>
            println(s"Failed with error: $f")
        }
      }
    }
}
