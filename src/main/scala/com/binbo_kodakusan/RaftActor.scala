package com.binbo_kodakusan

import akka.actor.{Actor, Cancellable}
import akka.util.Timeout
import akka.pattern.ask
import com.github.nscala_time.time.Imports._
import scala.concurrent.ExecutionContext
import scala.util.Random

object RaftActor {
  var map = collection.mutable.Map.empty[ServerId, ServerPersistentState]
}

// サーバの代わりにActorでシミュレート
class RaftActor(mySettingIndex: Int, serverSettings: Array[ServerSetting]) extends Actor {
  import scala.concurrent.duration._
  implicit val ec: ExecutionContext = context.system.dispatcher

  val electionTimeout = 500
  implicit val timeout = Timeout(electionTimeout milliseconds)
  var electionFrom = DateTime.now
  val MaxRandomize = 400

  val mySetting: ServerSetting = serverSettings(mySettingIndex)
  val rand = new Random

  var role: Role = Follower
  RaftActor.map.synchronized {
    RaftActor.map += (mySetting.serverId -> ServerPersistentState(Term(0), ServerNone, Vector()))
  }
  val serverPersistentState = RaftActor.map(mySetting.serverId)
  val serverState = ServerState(0, 0)
  val leaderState = LeaderState(Array.fill(serverSettings.length)(1), Array.fill(serverSettings.length)(0))

  private var scheduler: Cancellable = _

  // タイマー発行
  override def preStart() = scheduler = context.system.scheduler.schedule(0 millisecond, 10 milliseconds, context.self, Timer())
  override def postStop() = scheduler.cancel()

  private def commitRemain(): Unit = {
    while (serverState.commitIndex > serverState.lastApplied) {
      // if commitIndex > lastApplied: increment lastApplied, apply
      // log[lastApplied] to state machine (§5.3)
      serverState.lastApplied += 1
      val entry = serverPersistentState.log(serverState.lastApplied - 1)
      log(s"commit entry $entry(${serverState.lastApplied})")
    }
  }

  private def checkTerm(leaderId: ServerId, term: Term): Boolean = {
    // If RPC request or response contains term T > currentTerm:
    // set currentTerm = T, convert to follower (§5.1)
    if (term > serverPersistentState.currentTerm) {
      log(s"my term is old, change to follower")
      serverPersistentState.currentTerm = term
      log(s"role $role -> $Follower")
      role = Follower
      log(s"votedFor1 ${serverPersistentState.votedFor} -> $leaderId")
      serverPersistentState.votedFor = leaderId
      true
    } else
      false
  }

  private def startElection(): Unit = {
    // On conversion to candidate, start election:
    // - Increment currentTerm
    // - Vote for self
    // - Reset election timer
    // - Send RequestVote RPCs to all other servers
    serverPersistentState.currentTerm = serverPersistentState.currentTerm.nextTerm
    log(s"votedFor2 ${serverPersistentState.votedFor} -> ${mySetting.serverId}")
    serverPersistentState.votedFor = mySetting.serverId
    electionFrom = DateTime.now
    val lastIndex = serverPersistentState.log.length

    var voteCount = 0
    serverSettings.foreach{ s =>
      val actor = s.actor
      log(s"send RequestVote(to ${s.serverId})(${serverPersistentState.currentTerm}, ${mySetting.serverId}, ${serverPersistentState.currentTerm})")
      val f = actor ? RequestVote(serverPersistentState.currentTerm, mySetting.serverId, lastIndex, serverPersistentState.currentTerm)
      f onSuccess {
        case r @ RequestVoteReply(term, voteGranted) =>
          log(s"received(from ${s.serverId}): $r")
          // RequestVoteReply(Others -> Candidate)
          if (voteGranted) {
            voteCount += 1
            if (role == Candidate && voteCount > serverSettings.length / 2) {
              log(s"role $role -> $Leader")
              role = Leader
              log(s"I am Leader")
              for (i <- 0 until serverSettings.length) {
                leaderState.nextIndex(i) = serverPersistentState.log.length + 1
                leaderState.matchIndex(i) = 0
              }
              // Upon election: send initial empty AppendEntries RPCs
              // (heartbeat) to each server; repeat during idle periods to
              // prevent election timeouts (§5.2)
              for (i <- 0 until serverSettings.length) {
                if (serverSettings(i).serverId != mySetting.serverId)
                  sendAppendEntries(i)
              }
            }
          }
          commitRemain
      }
    }
  }

  private def sendAppendEntries(i: Int): Unit = {
    // If last log index ≥ nextIndex for a follower: send
    // AppendEntries RPC with log entries starting at nextIndex
    // - If successful: update nextIndex and matchIndex for
    //   follower (§5.3)
    // - If AppendEntries fails because of log inconsistency:
    //   decrement nextIndex and retry (§5.3)
    val nextIndex = leaderState.nextIndex(i)
    assert(nextIndex > 0)
    val prevNextIndex = nextIndex
    val lastIndex = serverPersistentState.log.length
    leaderState.nextIndex(i) = lastIndex + 1
    val sendLog = serverPersistentState.log.slice(if (nextIndex <= 0) 0 else nextIndex - 1, lastIndex)

    val prevIndex = nextIndex - 1
    assert(prevIndex >= 0)
    val prevTerm = if (prevIndex > 0) serverPersistentState.log(prevIndex - 1)._1 else Term(0)

    if (sendLog.length > 0)
      log(s"send AppendEntries(nextIndex=$nextIndex,matchIndex=${leaderState.matchIndex(i)},lastIndex=$lastIndex,prevIndex=$prevIndex,prevTerm=$prevTerm,sendLog=${sendLog.map(_._2.something).mkString(",")}) to ${serverSettings(i)}")
    val f = serverSettings(i).actor ? AppendEntries(serverPersistentState.currentTerm, mySetting.serverId, prevIndex, prevTerm, sendLog, serverState.commitIndex)
    f onSuccess {
      case r @ AppendEntriesReply(term, success) if role == Leader =>
        if (sendLog.length > 0)
          log(s"received: $r")
        // AppendEntriesReply(Followers -> Leader)
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
  }

  def receive = {
    case Timer() =>
      role match {
        case Follower =>
          // If election timeout elapses without receiving AppendEntries
          // RPC from current leader or granting vote to candidate:
          // convert to candidate
          if ((electionFrom to DateTime.now).toDurationMillis > electionTimeout) {
            log(s"election timeout, change to candidate")
            log(s"role $role -> $Candidate")
            role = Candidate
            log(s"votedFor2 ${serverPersistentState.votedFor} -> ${mySetting.serverId}")
            serverPersistentState.votedFor = mySetting.serverId
            electionFrom = DateTime.now
            self ! StartElection()
          }
        case Candidate =>
          // If election timeout elapses: start new election
          if ((electionFrom to DateTime.now).toDurationMillis > electionTimeout) {
            log(s"election timeout, retry to election")
            log(s"votedFor2 ${serverPersistentState.votedFor} -> $ServerNone")
            serverPersistentState.votedFor = ServerNone
            electionFrom = DateTime.now
            val d = (rand.nextInt(100) * 10) % MaxRandomize
            log(s"duration = $d milliseconds")
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
          val lastIndex = serverPersistentState.log.length
          for (i <- 0 until serverSettings.length) {
            if (serverSettings(i).serverId != mySetting.serverId) {
              val nextIndex = leaderState.nextIndex(i)
              sendAppendEntries(i)
            }
          }
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
              log (s"matchIndex=$matchIndex,n=$n,count=$count")
            }
          }
          if (count > serverSettings.length / 2) {
            log(s"commitIndex: ${serverState.commitIndex} -> $n")
            serverState.commitIndex = n
          }
      }
    case r @ GetLog() =>
      log(s"received: $r")
      sender ! GetLogReply(mySetting.serverId, serverPersistentState.log)
    case r @ StartElection() if role == Candidate =>
      log(s"received: $r")
      startElection
    case r @ RequestFromClient(command) =>
      log(s"received: $r")
      // If command received from client: append entry to local log,
      // respond after entry applied to state machine (§5.3)
      if (role == Leader) {
        serverPersistentState.log = serverPersistentState.log :+ (serverPersistentState.currentTerm, command)
        serverState.commitIndex += 1
        commitRemain
        // FIXME: 過半数がコミットしてから返答する？
        sender ! ReplyToClient(this.toString)
      } else {
        // FIXME: 毎回Actorを検索しないようにする
        if (role != Leader) {
          serverSettings.find(_.serverId == serverPersistentState.votedFor) match {
            case Some(s) =>
              val path = s"/user/main/raftActor-${s.serverId.id}"
              log(s"-> $path")
              val actor = context.system.actorSelection(path)
              val f = actor ? r
              f onSuccess {
                case rr@ReplyToClient(dummy) => sender ! rr
              }
            case _ =>
              log(s"not found leader: ${serverPersistentState.votedFor}")
              sender ! "ERROR"
          }
        } else {
          log(s"not found leader: ${serverPersistentState.votedFor}")
          sender ! "ERROR"
        }
      }
    case r @ AppendEntries(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit) if role == Leader || role == Candidate =>
      if (entries.length > 0)
        log(s"received: $r")
      // AppendEntries(Leader -> Follower)
      electionFrom = DateTime.now
      if (!checkTerm(leaderId, term)) {
        if (term >= serverPersistentState.currentTerm) {
          // If AppendEntries RPC received from new leader: convert to
          // follower
          log(s"other node is leader, change to follower")
          serverPersistentState.currentTerm = term
          log(s"role $role -> $Follower")
          role = Follower
          log(s"votedFor3 ${serverPersistentState.votedFor} -> $leaderId")
          serverPersistentState.votedFor = leaderId
        }
      }
      commitRemain
    case r @ AppendEntries(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit) if role == Follower =>
      if (entries.length > 0)
        log(s"received: $r")
      // AppendEntries(Leader -> Follower)
      electionFrom = DateTime.now
      if (!checkTerm(leaderId, term)) {
        if (term < serverPersistentState.currentTerm) {
          // Reply false if term < currentTerm (§5.1)
          sender ! AppendEntriesReply(serverPersistentState.currentTerm, false)
        } else if (prevLogIndex > serverPersistentState.log.length ||
          (prevLogIndex > 0 && prevLogIndex <= serverPersistentState.log.length &&
            prevLogTerm != serverPersistentState.log(prevLogIndex - 1)._1)) {
          // Reply false if log doesn’t contain an entry at prevLogIndex
          // whose term matches prevLogTerm (§5.3)
          sender ! AppendEntriesReply(serverPersistentState.currentTerm, false)
        } else {
          var index = prevLogIndex
          entries.foreach { entry =>
            if (index > 0 && index <= serverPersistentState.log.length &&
              entry._1 != serverPersistentState.log(index - 1)._1) {
              // if an existing entry conflicts with a new one (same index
              // but different terms), delete the existing entry and all that
              // follow it (§5.3)
              serverPersistentState.log = serverPersistentState.log.take(index - 1)
            }
            // append any new entries not already in the log
            serverPersistentState.log = serverPersistentState.log :+ entry
            log(s"appnd entry $entry($index)")
            index += 1
          }
          // if leaderCommit > commitIndex, set commitIndex =
          // min(leaderCommit, index of last new entry)
          val lastIndex = serverPersistentState.log.length
          if (leaderCommit > serverState.commitIndex)
            serverState.commitIndex = math.min(leaderCommit, lastIndex)
          sender ! AppendEntriesReply(serverPersistentState.currentTerm, true)
        }
      }
      commitRemain
    case r @ RequestVote(term, candidateId, lastLogIndex, lastLogTerm) =>
      // RequestVote(Candidate -> Others)
      electionFrom = DateTime.now
      if (term < serverPersistentState.currentTerm) {
        // Reply false if term < currentTerm (§5.1)
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
        log(s"votedFor ${serverPersistentState.votedFor} -> $candidateId")
        serverPersistentState.votedFor = candidateId
        sender ! RequestVoteReply(serverPersistentState.currentTerm, true)
      } else {
        sender ! RequestVoteReply(serverPersistentState.currentTerm, false)
      }
      commitRemain
  }

  def log(log: String) = {
    println(s"${mySetting.serverId}($role)(votedFor=${serverPersistentState.votedFor})(commitIndex=${serverState.commitIndex}, lastApplied=${serverState.lastApplied}): $log")
  }
}
