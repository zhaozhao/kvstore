package kvstore

import akka.actor.{ OneForOneStrategy, Props, ActorRef, Actor }
import kvstore.Arbiter._
import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart
import scala.annotation.tailrec
import akka.pattern.{ ask, pipe }
import akka.actor.Terminated
import scala.concurrent.duration._
import akka.actor.PoisonPill
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy
import akka.util.Timeout

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  var expectedSeq = 0L

  var acks = Map.empty[Long, (ActorRef, Snapshot)]

  var checks = Map.empty[Long, (Boolean, Set[ActorRef], ActorRef, Replicate)]

  val persistence = context.actorOf(persistenceProps, "persistence")

  arbiter ! Join

  val tick =
    context.system.scheduler.schedule(100 millis, 100 millis, self, Timeout)

  override def postStop() = tick.cancel()

  def sendReplicate(replicate:Replicate): Unit = {
    for (replicator <- replicators) {
      replicator ! replicate
    }
  }

  def checkFinish(id: Long): Unit ={
    val (persisted, replicators, client, _) = checks(id)

    if (persisted && replicators.isEmpty) {
      checks = checks - id
      client ! OperationAck(id)
    }
  }

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case Insert(key, value, id) => {
      kv = kv updated (key, value)
      val snapshot = Replicate(key, Some(value), id)
      checks = checks updated (id, (false, replicators, sender, snapshot))
      sendReplicate(snapshot)
      context.system.scheduler.scheduleOnce(1 second, self, id)
    }
    case Remove(key, id) => {
      kv = kv - key
      val replicate = Replicate(key, None, id)
      checks = checks updated (id, (false, replicators, sender, replicate))
      sendReplicate(replicate)
      context.system.scheduler.scheduleOnce(1 second, self, id)
    }
    case Persisted(key, id) => {
      if (checks contains id) {
        val (_, replicators, client, replicate) = checks(id)
        checks = checks updated (id, (true, replicators, client, replicate))
        checkFinish(id)
      }
    }
    case Replicated(key, id) => {
      if (checks contains id) {
        val (persisted, replicators, client, replicate) = checks(id)
        checks = checks updated (id, (persisted, replicators-sender, client, replicate))
        checkFinish(id)
      }

    }
    case id:Long => {
      if (checks contains id) {
        val (_, _, client, _) = checks(id)
        checks = checks - id
        client ! OperationFailed(id)
      }
    }
    case Get(key, id) => {
      sender ! GetResult(key, kv.get(key), id)
    }
    case Timeout => {
      checks foreach {
        case (seqNum, (persisted, _,_,Replicate(key, valueOption, id))) => {
          persistence ! Persist(key, valueOption, seqNum)
        }
      }
    }
    case Replicas(replicas) => {
      val replicasRemoveSelf = replicas.filter( _ != self)
      val new_secondaries = replicasRemoveSelf diff secondaries.keySet
      val remove_secondaries = secondaries.keySet diff replicasRemoveSelf
      for (secondary <- new_secondaries) {
        val replicator = context.actorOf(Props(classOf[Replicator], secondary))
        replicators = replicators + replicator
        secondaries = secondaries updated (secondary, replicator)
        for ((key, value) <- kv){
          replicator ! Replicate(key, Some(value), 0L)

        }
      }

      for (secondary <- remove_secondaries) {
        val replicator = secondaries(secondary)
        secondaries = secondaries - secondary
        replicators = replicators - replicator
        replicator ! PoisonPill
        checks = checks map {
          case (id, (persisted, old_replicators, client, replicate)) => (
            id, (persisted,old_replicators-replicator,client,replicate)
            )
        }
      }
      for (id <- checks.keys) {
        checkFinish(id)
      }
    }
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case Snapshot(key, valueOption, seq) => {
      if (seq < expectedSeq)
        sender ! SnapshotAck(key, seq)
      else if (seq == expectedSeq) {
        valueOption match {
          case None => kv = kv - key
          case Some(value) => kv = kv updated (key, value)
        }
        acks = acks updated (seq, (sender, Snapshot(key, valueOption, seq)))
        context.system.scheduler.scheduleOnce(1 second, self, seq)
        persistence ! Persist(key, valueOption, seq)
      }
    }
    case Persisted(key, seq) => {
      if (seq == expectedSeq) {
        val (replicator, snapshot) = acks(seq)
        acks = acks - seq

        expectedSeq = expectedSeq + 1
        replicator ! SnapshotAck(key, seq)
      }
    }
    case Get(key, id) => {
      sender ! GetResult(key, kv.get(key), id)
    }
    case seq: Long => {
      if (acks contains seq) {
        acks = acks - seq
        expectedSeq = expectedSeq + 1
      }
    }
    case Timeout => {
      acks foreach {
        case (seqNum, (_, Snapshot(key, valueOption, id))) => {
          persistence ! Persist(key, valueOption, seqNum)
        }
      }
    }
  }

}

