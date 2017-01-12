package com.binbo_kodakusan

// In Search of an Understandable Consensus Algorithm
// https://raft.github.io/raft.pdf

import java.util.concurrent.atomic.AtomicBoolean

import akka.actor.SupervisorStrategy._
import akka.actor.{Actor, ActorRef, ActorSystem, Cancellable, DiagnosticActorLogging, OneForOneStrategy, Props}
import akka.event.Logging
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Random

// サーバの設定
case class ServerSetting(val serverId: SpecificId, var actor: ActorRef)

class MainActor(system: ActorSystem, nodeCount: Int, messageCount: Int, dispatcher: String, settings: Array[ServerSetting]) extends Actor with DiagnosticActorLogging {
  import scala.concurrent.duration._
  implicit val ec: ExecutionContext = context.dispatcher
  implicit val t = Timeout(DurationInt(5) seconds)

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1 millisecond) {
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
    context.system.scheduler.scheduleOnce(1 second, self, SendTest(1))
  }
  override def postStop() = {
    scheduler.cancel()
    log.info(s"postStop = $this" + Array.fill(40)("-").mkString(""))
  }

  var map = Map.empty[ServerId, (Role, ServerId, Array[(Term, Command)])]

  def receive = {
    case ReplyToClient(dummy) => log.info(s"!!! $dummy")
    case GetLogReply(serverId, role, votedFor, log2) => log.info(s"!!! $serverId, $role, $votedFor, $log2")
    case SendTest(n) =>
      val actor = as(0)
      val f = actor ? RequestFromClient(Command(n.toString))
      f onSuccess {
        case ReplyToClient(s) => //log.info(s.toString)
      }
      if (n < messageCount)
        context.system.scheduler.scheduleOnce(10 milliseconds, self, SendTest(n + 1))
    case Timer() =>
      for (actor <- as) {
        val f = actor ? GetLog()
        f onSuccess {
          case GetLogReply(serverId, role, votedFor, log2) =>
            map.synchronized {
              map = map + (serverId -> (role, votedFor, log2.toArray))
            }
        }
      }
      log.info("---------------------------------------------")
      map.synchronized {
        map.foreach { kv =>
          log.info(s"${kv._1} -> (${kv._2._1}, ${kv._2._2}, ${kv._2._3.length})")
        }
        if (!map.isEmpty && map.forall { case (serverId, (role, votedFor, log)) =>
          log.length >= messageCount
        }) {
          map.foreach { kv =>
            log.info(s"${kv._1} -> (${kv._2._1}, ${kv._2._2}, ${kv._2._3.map(_._2.something).mkString(",")})")
          }
          log.info("children stopping")
          as.foreach(context.stop(_))
          context.stop(self)
          system.terminate
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
