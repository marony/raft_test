package com.binbo_kodakusan

// In Search of an Understandable Consensus Algorithm
// https://raft.github.io/raft.pdf

import java.util.concurrent.atomic.AtomicBoolean

import akka.actor.SupervisorStrategy._
import akka.actor.{Actor, ActorRef, ActorSystem, Cancellable, DiagnosticActorLogging, OneForOneStrategy, Props}
import akka.event.Logging
import akka.pattern.ask
import akka.util.Timeout
import org.joda.time.DateTime

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Random

// サーバの設定
case class ServerSetting(val serverId: SpecificId, var actor: ActorRef)

class MainActor(system: ActorSystem, nodeCount: Int, messageCount: Int, dispatcher: String, settings: Array[ServerSetting]) extends Actor with DiagnosticActorLogging {
  import scala.concurrent.duration._
  implicit val ec: ExecutionContext = context.dispatcher
  implicit val timeout = Timeout(DurationInt(5) seconds)

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1 minute) {
      case _ => Restart
    }

  // 子供Actorを起動
  var as: Vector[ActorRef] = Vector()
  for (i <- 0 until settings.length) {
    val path = s"raftActor-${settings(i).serverId.id}"
    val actor = context.actorOf(Props(new RaftActor(i, settings)).withDispatcher(dispatcher), name = path)
    settings(i).actor = actor
    as = as :+ actor
  }

  val rand = new Random

  private var scheduler: Cancellable = _

  // タイマー発行
  override def preStart() = {
    log.info(s"preStart = $this" + Array.fill(40)("-").mkString(""))
    scheduler = context.system.scheduler.schedule(0 millisecond, 5 seconds, context.self, Timer())
    context.system.scheduler.scheduleOnce(5 second, self, SendTest(1))
  }
  override def postStop() = {
    scheduler.cancel()
    log.info(s"postStop = $this" + Array.fill(40)("-").mkString(""))
  }

  val start = DateTime.now
  var map = Map.empty[ServerId, (Role, ServerId, Int, Int, Array[(Term, Command)])]

  var successCount = 0
  var failureCount = 0

  def receive = {
    case ReplyToClient(dummy) => log.info(s"!!! $dummy")
    case GetLogReply(serverId, role, votedFor, commitIndex, lastApplied, log2) => log.info(s"!!! $serverId, $role, $votedFor, $commitIndex, $lastApplied, $log2")
    case SendTest(n) =>
      val actor = as(0)
      val f = actor ? RequestFromClient(Command(n.toString))
      f onSuccess {
        case ReplyToClient(s) =>
          if (s)
            successCount += 1
          else
            failureCount += 1
          log.debug(s"A: $s, ReplyToClient: $successCount, $failureCount, $s")
      }
      f onFailure {
        case e =>
          failureCount += 1
//          log.error(s"B: $n, ReplyToClient: $successCount, $failureCount, $e, ${e.getStackTrace.map(s => s"${s.getClassName}:${s.getMethodName}(${s.getFileName}/${s.getLineNumber})").mkString(",")}")
      }
      if (n < messageCount)
        context.system.scheduler.scheduleOnce(10 milliseconds, self, SendTest(n + 1))
    case Timer() =>
      for (actor <- as) {
        val f = actor ? GetLog()
        f onSuccess {
          case GetLogReply(serverId, role, votedFor, commitIndex, lastApplied, log2) =>
            map.synchronized {
              map = map + (serverId -> (role, votedFor, commitIndex, lastApplied, log2.toArray))
            }
        }
      }
      log.info("---------------------------------------------")
      map.synchronized {
        // 各サーバの受信内容を出力
        map.foreach { kv =>
          log.info(s"${kv._1} -> (${kv._2._1}, ${kv._2._2}, ${kv._2._3}, ${kv._2._4}, ${kv._2._5.length})")
        }
        // 全データを受信したか一定時間経つまで待つ
        val flag = (DateTime.now.getMillis - start.getMillis) > 60 * 1000
        val count = messageCount// - failureCount
        if (!map.isEmpty && map.forall { case (serverId, (role, votedFor, commitIndex, lastApplied, log)) =>
          flag || log.length >= count && commitIndex == lastApplied && commitIndex >= count
        }) {
          // それぞれのサーバの受信内容を出力
          map.foreach { kv =>
            log.info(s"${kv._1} -> (${kv._2._1}, ${kv._2._2}, ${kv._2._3}, ${kv._2._4}, ${kv._2._5.map(_._2.something).mkString(",")}(${kv._2._5.length}))")
          }
          // 各サーバの受信していないデータと重複して受信したデータを出力
          var map2 = Map[ServerId, Vector[Int]]()
          var map3 = Map[ServerId, Map[Int, Int]]()
          for (i <- 1 to messageCount) {
            map.foreach { kv =>
              (kv._2._5.find(c => c._2.something == i.toString)) match {
                case Some(s) =>
                  var v = map3.getOrElse(kv._1, Map(i -> 0))
                  val c = v.getOrElse(i, 0)
                  v = v + (i -> (c + 1))
                  map3 += (kv._1 -> v)
                case None =>
                  val v = map2.getOrElse(kv._1, Vector.empty)
                  map2 += (kv._1 -> (v :+ i))
              }
            }
          }
          map2.foreach {
            case (serverId, v) =>
              println(s"$serverId: not found -> ${v.mkString(",")}")
          }
          map3.foreach {
            case (serverId, m) =>
              m.filter{ case (_, c) => c > 1 }.foreach {
                case (i, c) =>
                  println(s"$serverId: duplicated -> ${i}, ${c}")
              }
          }
          log.info("children stopping")
          as.foreach(context.stop(_))
          context.stop(self)
          system.terminate
          log.info(s"$successCount + $failureCount == $messageCount(${successCount + failureCount})")
        }
      }
  }
}

class Main {
}

object Main {
  def main(args: Array[String]): Unit = {
    val NodeCount = 3
    val MessageCount = 1000
    val dispatcher = "akka.actor.my-pinned-dispatcher"

    val system = ActorSystem("raft")
    val logger = Logging(system, classOf[Main])

    assert(system.dispatchers.hasDispatcher(dispatcher))
    val settings = (for (i <- 1 to NodeCount) yield ServerSetting(SpecificId(i), null)).toArray

    val actor = system.actorOf(Props(new MainActor(system, NodeCount, MessageCount, dispatcher, settings)).withDispatcher(dispatcher), name = "main")
    val flag = new AtomicBoolean(false)
    system.registerOnTermination {
      flag.set(true);
    }
    while (flag.get) {
      Thread.sleep(1000)
    }
    logger.info("[END]")
  }
}
