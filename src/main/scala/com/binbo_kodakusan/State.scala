package com.binbo_kodakusan

// サーバが持つ状態
case class ServerPersistentState(var currentTerm: Term, var votedFor: ServerId, var log: Vector[(Term, Command)])
case class ServerState(var commitIndex: Int, var lastApplied: Int)
case class LeaderState(var nextIndex: Array[Int], var matchIndex: Array[Int])

// サーバのID
sealed trait ServerId
case class SpecificId(val id: Int) extends ServerId
case object ServerNone extends ServerId

// term(リーダー選出のたびに変わる)
case class Term(val term: Int) {
  def nextTerm() = Term(term + 1)
}
object Term {
  import scala.math._

  implicit val ordering: Ordering[Term] = Ordering.by(o => o.term)
  implicit def orderingToOrdered(o: Term) = Ordered.orderingToOrdered(o)(ordering)
}
