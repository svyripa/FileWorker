import java.io.{File, PrintWriter}

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer

import scala.concurrent.Future
import scala.io.Source
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global

object DeductBigRightFromBigLeft extends App {
  val start = System.currentTimeMillis()

  // In memory
  val leftIter = Source.fromFile("all.csv", "UTF-8")
  val left = leftIter.getLines().toList

  // In memory
  val rightIter = Source.fromFile("all1.csv", "UTF-8")
  val right = rightIter.getLines().toList.groupBy(identity)

  val pw = new PrintWriter("deduct-big-right-from-big-Left.csv", "UTF-8")

  def isSentenceContains(sentence: List[String]): Boolean = sentence match {
    case x :: xs =>
      Try(right(x)) match {
        case Success(_) =>
          true
        case Failure(_) =>
          isSentenceContains(xs)
      }
    case Nil => false
  }

  import akka.stream.scaladsl.Source

  implicit val sys = ActorSystem("actor-system")
  implicit val materializer = ActorMaterializer()

  Source.fromIterator(() => left.iterator)
    .mapAsyncUnordered(32) { str =>
      Future {
        // Left consists of sentences
        //(isSentenceContains(str.split(' ').toList, str)
        // Left consists of whole words
        (isSentenceContains(List(str)), str)
      }
    }
    .filterNot(_._1)
    .map(_._2)
    .runForeach(pw.println)
    .andThen {
      case _try => {
        pw.close()
        rightIter.close()
        leftIter.close()
        _try match {
          case Success(s) =>
            println(s"Done successfully. Time: ${(System.currentTimeMillis().toDouble - start) / 1000}")
          case Failure(f) =>
            println(s"Failed with error: $f")
        }
      }
    }
}