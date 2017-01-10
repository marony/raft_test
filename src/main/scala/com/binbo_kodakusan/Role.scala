package com.binbo_kodakusan

// サーバの現在の役割
sealed trait Role
case object Initializing extends Role
case object Follower extends Role
case object Candidate extends Role
case object Leader extends Role
