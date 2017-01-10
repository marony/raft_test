package com.binbo_kodakusan

// サーバが持つ状態
case class ServerPersistentState(var currentTerm: Term, var votedFor: ServerId, var log: Vector[(Term, Command)])
case class ServerState(var commitIndex: Int, var lastApplied: Int)
case class LeaderState(var nextIndex: Array[Int], var matchIndex: Array[Int])
