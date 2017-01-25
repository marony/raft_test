package com.binbo_kodakusan

import akka.actor.{Actor, ActorLogging, Cancellable, DiagnosticActorLogging, PoisonPill}
import akka.event.Logging
import akka.util.Timeout
import akka.pattern.ask
import com.github.nscala_time.time.Imports._

import scala.collection.mutable
import scala.concurrent.{Await, ExecutionContext, Future, TimeoutException}
import scala.util.Random

object RaftActor {
  var map = collection.mutable.Map.empty[ServerId, ServerPersistentState]
}

// サーバの代わりにActorでシミュレート
class RaftActor(mySettingIndex: Int, serverSettings: Array[ServerSetting]) extends Actor with DiagnosticActorLogging {
  import scala.concurrent.duration._
  implicit val ec: ExecutionContext = context.system.dispatcher

  val electionTimeout = 2000
  implicit val timeout = Timeout(electionTimeout milliseconds)
  var electionFrom = DateTime.now
  val MaxRandomize = 400

  val mySetting: ServerSetting = serverSettings(mySettingIndex)
  val rand = new Random

  var role: Role = Follower

  val serverPersistentState = RaftActor.map.getOrElse(mySetting.serverId, ServerPersistentState(Term(0), ServerNone, Vector()))
  val serverState = ServerState(0, 0)
  val leaderState = LeaderState(Array.fill(serverSettings.length)(1), Array.fill(serverSettings.length)(0))

  private var scheduler: Cancellable = _

  // タイマー発行
  override def preStart() = {
    debug(s"preStart = $self" + Array.fill(40)("=").mkString(""))
    // Leaderだった自分が落ちたのでリセット
    if (serverPersistentState.votedFor == mySetting.serverId)
      serverPersistentState.votedFor = ServerNone
    scheduler = context.system.scheduler.schedule(0 millisecond, 10 milliseconds, context.self, Timer())
  }
  override def postStop() = {
    scheduler.cancel()
    RaftActor.map.synchronized {
      RaftActor.map += (mySetting.serverId -> serverPersistentState)
    }
    debug(s"postStop = $self" + Array.fill(40)("=").mkString(""))
  }

  // lastAppliedよりcommitIndexが進んでいたらコミットする(Leader, Candidate, Follower)
  private def commitRemain(): Unit = {
    while (serverState.lastApplied < serverPersistentState.log.length && serverState.commitIndex > serverState.lastApplied) {
      // if commitIndex > lastApplied: increment lastApplied, apply
      // log[lastApplied] to state machine (§5.3)
      serverState.lastApplied += 1
      val entry = serverPersistentState.log(serverState.lastApplied - 1)
    }
  }

  // 新しいTermでAppendEntriesを受信したら、Termを変更しFollowerに(Leader, Candidate, Follower)
  private def checkTerm(leaderId: ServerId, term: Term): Boolean = {
    // If RPC request or response contains term T > currentTerm:
    // set currentTerm = T, convert to follower (§5.1)
    if (term > serverPersistentState.currentTerm) {
      info(s"my term is old, change to follower")
      serverPersistentState.currentTerm = term
      info(s"role $role -> $Follower")
      role = Follower
      info(s"votedFor1 ${serverPersistentState.votedFor} -> $leaderId")
      serverPersistentState.votedFor = leaderId
      true
    } else
      false
  }

  // リーダー選出の開始(Candidate)
  private def startElection(): Unit = {
    // On conversion to candidate, start election:
    // - Increment currentTerm
    // - Vote for self
    // - Reset election timer
    // - Send RequestVote RPCs to all other servers
    serverPersistentState.currentTerm = serverPersistentState.currentTerm.nextTerm
    info(s"votedFor2 ${serverPersistentState.votedFor} -> ${mySetting.serverId}")
    serverPersistentState.votedFor = mySetting.serverId
    electionFrom = DateTime.now
    val lastIndex = serverPersistentState.log.length

    val fs = serverSettings.map { s =>
      val actor = s.actor
      info(s"send RequestVote(to ${s.serverId})(${serverPersistentState.currentTerm}, ${mySetting.serverId}, ${serverPersistentState.currentTerm})")
      (actor ? RequestVote(serverPersistentState.currentTerm, mySetting.serverId, lastIndex, serverPersistentState.currentTerm)).asInstanceOf[Future[RequestVoteReply]]
    }
    val f: Future[Seq[RequestVoteReply]] = Future.sequence(fs)
    f.onSuccess {
      case rs: Seq[RequestVoteReply] =>
        // クォーラム(過半数)以上の賛成があるか
        if (role == Candidate && rs.count(_.voteGranted) > serverSettings.length / 2) {
          info(s"role $role -> $Leader")
          role = Leader
          info(s"I am Leader")
          for (i <- 0 until serverSettings.length) {
            leaderState.nextIndex(i) = serverPersistentState.log.length + 1
            leaderState.matchIndex(i) = 0
          }
          // Upon election: send initial empty AppendEntries RPCs
          // (heartbeat) to each server; repeat during idle periods to
          // prevent election timeouts (§5.2)
          /*val f =*/ sendAppendEntries()
        }
        commitRemain
    }
  }

  // AppendEntries送信(Leader)
  private def sendAppendEntries(): Future[Seq[Boolean]] = {
    // If last log index ≥ nextIndex for a follower: send
    // AppendEntries RPC with log entries starting at nextIndex
    // - If successful: update nextIndex and matchIndex for
    //   follower (§5.3)
    // - If AppendEntries fails because of log inconsistency:
    //   decrement nextIndex and retry (§5.3)
    val fs = serverSettings.zipWithIndex.filter(_._1.serverId != mySetting.serverId).map { s =>
      val setting = s._1
      val i = s._2
      val nextIndex = leaderState.nextIndex(i)
      assert(nextIndex > 0)
      val prevNextIndex = nextIndex
      val lastIndex = serverPersistentState.log.length
      leaderState.nextIndex(i) = lastIndex + 1
      val sendLog = serverPersistentState.log.slice(if (nextIndex <= 0) 0 else nextIndex - 1, lastIndex)

      val prevIndex = nextIndex - 1
      assert(prevIndex >= 0)
      val prevTerm = if (prevIndex > 0) serverPersistentState.log(prevIndex - 1)._1 else Term(0)

      if (sendLog.length > 0) {
        debug(s"send AppendEntries: from $self to ${setting.actor}, ${AppendEntries(serverPersistentState.currentTerm, mySetting.serverId, prevIndex, prevTerm, sendLog, serverState.commitIndex)}")
      }
      val f = setting.actor ? AppendEntries(serverPersistentState.currentTerm, mySetting.serverId, prevIndex, prevTerm, sendLog, serverState.commitIndex)
      f onSuccess {
        case r @ AppendEntriesReply(term, success) if role == Leader =>
          // AppendEntriesReply(Followers -> Leader)
          if (sendLog.length > 0) {
            debug(s"received AppendEntriesReply from ${setting.actor} to $self, $r")
          }
          if (success) {
            if (leaderState.matchIndex(i) < lastIndex) {
              leaderState.matchIndex(i) = lastIndex
            }
          } else {
            // FIXME: retry without Timer
            if (prevNextIndex > 1)
              leaderState.nextIndex(i) = prevNextIndex - 1
          }
          commitRemain
      }
//      info(s"Future = $f")
      f.asInstanceOf[Future[AppendEntriesReply]].map(_.success)
    }
    Future.sequence(fs)
  }

  def receive = {
    case Timer() =>
      // テスト用にランダムで死ぬ
      val t = rand.nextInt(10000)
      if (t < 10) {
        info(s"死ぬよ！！ $self" + Array.fill(40)("=").mkString(""))
        throw new Exception("死ぬ")
      } else {
        role match {
          case Follower =>
            // If election timeout elapses without receiving AppendEntries
            // RPC from current leader or granting vote to candidate:
            // convert to candidate
            if ((electionFrom to DateTime.now).toDurationMillis > electionTimeout) {
              info(s"election timeout, change to candidate")
              info(s"role $role -> $Candidate")
              role = Candidate
              info(s"votedFor2 ${serverPersistentState.votedFor} -> ${mySetting.serverId}")
              serverPersistentState.votedFor = mySetting.serverId
              electionFrom = DateTime.now
              self ! StartElection()
            }
          case Candidate =>
            // If election timeout elapses: start new election
            if ((electionFrom to DateTime.now).toDurationMillis > electionTimeout) {
              info(s"election timeout, retry to election")
              info(s"votedFor2 ${serverPersistentState.votedFor} -> $ServerNone")
              serverPersistentState.votedFor = ServerNone
              electionFrom = DateTime.now
              val d = (rand.nextInt(100) * 10) % MaxRandomize
              debug(s"duration = $d milliseconds")
              context.system.scheduler.scheduleOnce(d milliseconds, self, StartElection())
            }
          case Leader =>
            // Upon election: send initial empty AppendEntries RPCs
            // (heartbeat) to each server; repeat during idle periods to
            // prevent election timeouts (§5.2)
            //
            // If last log index ≥ nextIndex for a follower: send
            // AppendEntries RPC with log entries starting at nextIndex
            // - If successful: update nextIndex and matchIndex for
            //   follower (§5.3)
            // - If AppendEntries fails because of log inconsistency:
            //   decrement nextIndex and retry (§5.3)
            /*val f =*/ sendAppendEntries()
            // If there exists an N such that N > commitIndex, a majority
            // of matchIndex[i] ≥ N, and log[N].term == currentTerm:
            // set commitIndex = N (§5.3, §5.4).
            var count = 0
            var n = Int.MaxValue
            for (i <- 0 until serverSettings.length) {
              val matchIndex = leaderState.matchIndex(i)
              if (matchIndex > serverState.commitIndex &&
                matchIndex > 0 && matchIndex <= serverPersistentState.log.length &&
                serverPersistentState.log(matchIndex - 1)._1 == serverPersistentState.currentTerm) {
                if (n > matchIndex)
                  n = matchIndex
                count += 1
                debug(s"matchIndex=$matchIndex,n=$n,count=$count")
              }
            }
            // クォーラム(過半数)以上成功したか(自分には送らないので-1)
            if (count > serverSettings.length / 2 - 1) {
              debug(s"commitIndex: ${serverState.commitIndex} -> $n")
              serverState.commitIndex = n
            }
        }
      }

    case r @ GetLog() =>
//      info(s"received: $r")
      sender ! GetLogReply(mySetting.serverId, role, serverPersistentState.votedFor, serverState.commitIndex, serverState.lastApplied, serverPersistentState.log)
    case r @ StartElection() if role == Candidate =>
      info(s"received: $r")
      startElection
    case r @ RequestFromClient(command) =>
      debug(s"received: from $sender to $self, $r")
      // If command received from client: append entry to local log,
      // respond after entry applied to state machine (§5.3)
      if (role == Leader) {
        serverPersistentState.log = serverPersistentState.log :+ (serverPersistentState.currentTerm, command)
        val f = sendAppendEntries()
        f onSuccess {
          case rs: Seq[Boolean] =>
            // クォーラム(過半数)以上成功したか(自分には送らないので-1)
//            debug(s"sendAppendEntries1: result = ${rs.mkString(",")}")
            if (rs.count(b => b) > serverSettings.length / 2 - 1) {
              serverState.commitIndex += 1
              commitRemain
              debug(s"sending: from $self to $sender, ${ReplyToClient(true)}")
              sender ! ReplyToClient(true)
            } else {
              debug(s"sending: from $self to $sender, ${ReplyToClient(false)}")
              sender ! ReplyToClient(false)
            }
        }
        f onFailure {
          case e =>
            // 失敗
            debug(s"sending: from $self to $sender, ${ReplyToClient(true)}")
            sender ! ReplyToClient(false)
        }
      } else {
        if (role != Leader) {
          serverSettings.find(s => s.serverId != mySetting.serverId && s.serverId == serverPersistentState.votedFor) match {
            case Some(s) =>
              debug(s"sending: from $self to ${s.actor}, $r")
              val f = s.actor ? r
              f onSuccess {
                case rr@ReplyToClient(dummy) =>
                  debug(s"sending: from $self to $sender, $rr")
                  sender ! rr
              }
              f onFailure {
                case e =>
                  debug(s"sending: from $self to $sender, ${ReplyToClient(true)}")
                  sender ! ReplyToClient(false)
              }
            case _ =>
              error(s"not found leader1: ${serverPersistentState.votedFor}, $command")
              debug(s"sending: from $self to $sender, ${ReplyToClient(true)}")
              sender ! ReplyToClient(false)
          }
        } else {
          error(s"not found leader2: ${serverPersistentState.votedFor}, $command")
          debug(s"sending: from $self to $sender, ${ReplyToClient(true)}")
          sender ! ReplyToClient(false)
        }
      }
    case r @ AppendEntries(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit) if role == Leader || role == Candidate =>
      // AppendEntries(Leader -> Follower)
      if (entries.length > 0) {
        debug(s"received AppendEntries1(${mySetting.serverId})($role)($term, $leaderId, $prevLogIndex, $prevLogTerm, $leaderCommit)(from $sender): $r")
      }
      electionFrom = DateTime.now
      if (!checkTerm(leaderId, term)) {
        if (term >= serverPersistentState.currentTerm) {
          // If AppendEntries RPC received from new leader: convert to
          // follower
          info(s"other node is leader, change to follower")
          serverPersistentState.currentTerm = term
          info(s"role $role -> $Follower")
          role = Follower
          info(s"votedFor3 ${serverPersistentState.votedFor} -> $leaderId")
          serverPersistentState.votedFor = leaderId
        }
      }
      commitRemain
    case r @ AppendEntries(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit) if role == Follower =>
      // AppendEntries(Leader -> Follower)
//      if (entries.length > 0) {
//        debug(s"received AppendEntries2(${mySetting.serverId})($role)($term, $leaderId, $prevLogIndex, $prevLogTerm, $leaderCommit)(from $sender): $r")
//        info(s"clog = ${serverPersistentState.log.mkString(",")}")
//      }
      electionFrom = DateTime.now
      // FIXME: checkTermに関係なく先に進んでいいかも
      if (!checkTerm(leaderId, term)) {
        if (term < serverPersistentState.currentTerm) {
          // Reply false if term < currentTerm (§5.1)

          // ログのtermが古い
          sender ! AppendEntriesReply(serverPersistentState.currentTerm, false)
        } else if (prevLogIndex > serverPersistentState.log.length ||
                   (prevLogIndex > 0 && prevLogIndex <= serverPersistentState.log.length &&
                    prevLogTerm != serverPersistentState.log(prevLogIndex - 1)._1)) {
          // Reply false if log doesn’t contain an entry at prevLogIndex
          // whose term matches prevLogTerm (§5.3)

          // prevLogIndexが存在しないか、termが異なっている
          sender ! AppendEntriesReply(serverPersistentState.currentTerm, false)
        } else {
          // 正常
          var index = prevLogIndex + 1
          entries.foreach { entry =>
            if (index > 0 && index <= serverPersistentState.log.length &&
              entry._1 != serverPersistentState.log(index - 1)._1) {
              // if an existing entry conflicts with a new one (same index
              // but different terms), delete the existing entry and all that
              // follow it (§5.3)
              serverPersistentState.log = serverPersistentState.log.take(index - 1)
            }
            // append any new entries not already in the log
            if (serverPersistentState.log.length > 0 &&
                serverPersistentState.log(serverPersistentState.log.length - 1)._2.something.toInt + 1 != entry._2.something.toInt) {
              // 値がおかしい
              error(s"Invalid value = ${serverPersistentState.log(serverPersistentState.log.length - 1)}, $entry")
            }
            serverPersistentState.log = serverPersistentState.log :+ entry
            index += 1
          }
          // if leaderCommit > commitIndex, set commitIndex =
          // min(leaderCommit, index of last new entry)

          // LeaderのcommitIndexを採用する
          val lastIndex = serverPersistentState.log.length
          if (leaderCommit > serverState.commitIndex)
            serverState.commitIndex = math.min(leaderCommit, lastIndex)
          sender ! AppendEntriesReply(serverPersistentState.currentTerm, true)
        }
      }
      commitRemain
    case r @ RequestVote(term, candidateId, lastLogIndex, lastLogTerm) =>
      // RequestVote(Candidate -> Others)
      info(s"RequestVote: votedFor = ${serverPersistentState.votedFor}, currentTerm = ${serverPersistentState.currentTerm}, lastLogTerm = $lastLogTerm, logLength = ${serverPersistentState.log.length}, lastLogIndex = $lastLogIndex")
      electionFrom = DateTime.now
      if (term < serverPersistentState.currentTerm) {
        // Reply false if term < currentTerm (§5.1)

        // termが古い
        sender ! RequestVoteReply(serverPersistentState.currentTerm, false)
      } else if ((serverPersistentState.votedFor == ServerNone ||
                  serverPersistentState.votedFor == candidateId) &&
                 (serverPersistentState.currentTerm < lastLogTerm ||
                  (serverPersistentState.currentTerm == lastLogTerm &&
                   serverPersistentState.log.length <= lastLogIndex))) {
        // if votedFor is null or candidateId, and candidate’s log is at
        // least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
        // §5.4.1
        // Raft determines which of two logs is more up-to-date
        // by comparing the index and term of the last entries in the
        // logs. If the logs have last entries with different terms, then
        // the log with the later term is more up-to-date. If the logs
        // end with the same term, then whichever log is longer is
        // more up-to-date.

        // 初めての選出か、自分が選出したserverIdで
        // termが新しいか、termは同じでログが多い
        info(s"votedFor ${serverPersistentState.votedFor} -> $candidateId")
        serverPersistentState.votedFor = candidateId
        sender ! RequestVoteReply(serverPersistentState.currentTerm, true)
      } else {
        sender ! RequestVoteReply(serverPersistentState.currentTerm, false)
      }
      commitRemain
  }

  def debug(text: String) = {
    log.debug(s"${mySetting.serverId}($role)(${serverState.commitIndex},${serverState.lastApplied}): $text")
  }
  def info(text: String) = {
    log.info(s"${mySetting.serverId}($role)(${serverState.commitIndex},${serverState.lastApplied}): $text")
  }
  def error(text: String) = {
    log.error(s"${mySetting.serverId}($role)(${serverState.commitIndex},${serverState.lastApplied}): $text")
  }
}
