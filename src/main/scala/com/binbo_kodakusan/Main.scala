package com.binbo_kodakusan

// In Search of an Understandable Consensus Algorithm
// https://raft.github.io/raft.pdf

import akka.actor.{Actor, ActorRef, ActorSystem, Cancellable, Props}
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Random

// サーバの設定
case class ServerSetting(val serverId: SpecificId, var actor: ActorRef)

class MainActor(nodeCount: Int, messageCount: Int, dispatcher: String, settings: Array[ServerSetting]) extends Actor {
  // 子供Actorを起動
  var as: Vector[ActorRef] = Vector()
  for (i <- 0 until settings.length) {
    val path = s"raftActor-${settings(i).serverId.id}"
    val actor = context.actorOf(Props(new RaftActor(i, settings)).withDispatcher(dispatcher), name = path)
    settings(i).actor = actor
    as = as :+ actor
  }

  Thread.sleep(3 * 1000)

  import scala.concurrent.duration._
  implicit val ec: ExecutionContext = context.dispatcher
  implicit val t = Timeout(DurationInt(5) seconds)

  val rand = new Random
  val actor = as(0)
  for (i <- 1 to messageCount) {
    val f = actor ? RequestFromClient(Command(i.toString))
    f onSuccess {
      case ReplyToClient(s) => //println(s)
    }
    Thread.sleep(rand.nextInt(100))
  }

  private var scheduler: Cancellable = _

  // タイマー発行
  override def preStart() = scheduler = context.system.scheduler.schedule(0 millisecond, 5 seconds, context.self, Timer())
  override def postStop() = scheduler.cancel()

  var map = Map.empty[ServerId, Array[(Term, Command)]]

  def receive = {
    case ReplyToClient(dummy) => println(s"!!! $dummy")
    case GetLogReply(serverId, log) => println(s"!!! $serverId, $log")
    case Timer() =>
      for (actor <- as) {
        val f = actor ? GetLog()
        f onSuccess {
          case GetLogReply(serverId, log) =>
            map.synchronized {
              map = map + (serverId -> log.toArray)
            }
        }
      }
      println("---------------------------------------------")
      map.synchronized {
        map.foreach { kv =>
          println(s"${kv._1} -> ${kv._2.length}")
        }
      }
  }
}

object Main {
  def main(args: Array[String]): Unit = {
    val NodeCount = 9
    val MessageCount = 10000
    val dispatcher = "akka.actor.my-pinned-dispatcher"

    val system = ActorSystem("raft")
    assert(system.dispatchers.hasDispatcher(dispatcher))
    val settings = (for (i <- 1 to NodeCount) yield ServerSetting(SpecificId(i), null)).toArray

    val actor = system.actorOf(Props(new MainActor(NodeCount, MessageCount, dispatcher, settings)).withDispatcher(dispatcher), name = "main")
    Thread.sleep(120 * 1000)
  }
}
