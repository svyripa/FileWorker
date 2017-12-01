import java.io.{File, PrintWriter}

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer

import scala.concurrent.Future
import scala.io.Source
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global

object DeductRightFromLeft extends App {
  val start = System.currentTimeMillis()

  // Not in memory, iterator
  val leftIter = Source.fromFile("all.csv", "UTF-8")
  val left = leftIter.getLines()

  // In memory. Usually it's pretty small
  val rightIter = Source.fromFile("stop_words.tsv", "UTF-8")
  val right = rightIter.getLines().toList

  val pw = new PrintWriter("deduct-right-from-Left.csv", "UTF-8")

  def isRightContains(word: String, rightX: List[String]): Boolean = rightX match {
    case x :: xs =>
      if (x == word) true
      else isRightContains(word, xs)
    case Nil => false
  }

  def isSentenceContains(sentence: List[String]): Boolean = sentence match {
    case x :: xs =>
      if (isRightContains(x, right)) true
      else isSentenceContains(xs)
    case Nil => false
  }

  import akka.stream.scaladsl.Source

  implicit val sys = ActorSystem("actor-system")
  implicit val materializer = ActorMaterializer()

  Source.fromIterator(() => left)
    .mapAsyncUnordered(32) { str =>
      Future {
        (isSentenceContains(str.split(' ').toList), str)
        //(str.split(' ').exists(right.contains), str)
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