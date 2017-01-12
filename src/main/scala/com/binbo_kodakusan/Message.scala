package com.binbo_kodakusan

sealed trait Message

// サーバ間のメッセージ
// 要求
sealed trait Request extends Message
case class GetLog() extends Request
case class Timer() extends Request
case class StartElection() extends Request
case class RequestFromClient(comand: Command) extends Request
case class AppendEntries(val term: Term, val leaderId: ServerId, val prevLogIndex: Int, val prevLogTerm: Term, val entries: Seq[(Term, Command)], val leaderCommit: Int) extends Request
case class RequestVote(val term: Term, val candidateId: ServerId, val lastLogIndex: Int, val lastLogTerm: Term) extends Request
case class SendTest(val n: Int)
// 返答
sealed trait Reply extends Message
case class GetLogReply(val serverId: ServerId, val role: Role, val votedFor: ServerId, val entries: Seq[(Term, Command)]) extends Reply
case class ReplyToClient(ok: Boolean) extends Reply
case class AppendEntriesReply(val term: Term, val success: Boolean) extends Reply
case class RequestVoteReply(val term: Term, val voteGranted: Boolean) extends Reply
