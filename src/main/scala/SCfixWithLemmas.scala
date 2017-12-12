import java.io.PrintWriter

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.matching.Regex
import scala.util.{Failure, Success}
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer

object SCfixWithLemmas extends App {
  val t1 = System.currentTimeMillis()

  implicit val system = ActorSystem("my-actor-system")
  implicit val materializer = ActorMaterializer()

  val dir = "/home/heroys6/Desktop/BK-608/"

  val sc: Iterator[String] = scala.io.Source
    .fromFile(dir + "bookimed_all_sc.csv", "utf-8")
    .getLines()

  val lemms: List[String] = scala.io.Source
    .fromFile(dir + "lemms.csv", "utf-8")
    .getLines()
    .toList
    .map(_.toLowerCase)
    .filterNot(_.length < 4)
    .filterNot(_ == "лечен")
    .filterNot(_ == "отзыв")
    .filterNot(_ == "центр")
    .filterNot(_ == "лучш")
    .filterNot(_ == "женщин")
    .filterNot(_ == "женск")

  val writer = new PrintWriter(dir + "sc-fixed.csv")

  val pattern: Regex = """[а-яА-Яa-zA-Z]+""".r

  Source.fromIterator(() => sc)
    .mapAsyncUnordered(32){ key =>
      Future {
        def firstBig(word: String): String = {
          word.toCharArray.toList match {
            case x :: xs =>
              x.toString.toUpperCase ++ xs.mkString
          }
        }
        def isContainsLem(word: String, lemmsX: List[String]): Boolean = lemmsX match {
          case x :: xs =>
            if (word.contains(x)) true
            else isContainsLem(word, xs)
          case Nil => false
        }
        def constructKey(key: String, words: List[String]): String = words match {
          case x :: xs =>
            //println("constructKey: " + x)
            val newKey =
              if (isContainsLem(x, lemms)) key.replace(x, firstBig(x))
              else key
            constructKey(newKey, xs)
          case Nil => key
        }

        val words = pattern.findAllIn(key).toList

        //println("Found words: " + words)

        constructKey(key, words)
      }
    }
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
