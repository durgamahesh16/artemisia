package tech.artemisia.core.dag

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.routing.BalancingPool
import com.typesafe.config.{Config, ConfigFactory}
import tech.artemisia.core.{AppContext, AppLogger, Keywords}

/**
 * Created by chlr on 1/18/16.
 */
class ActorSysManager(app_context: AppContext) {

  AppLogger debug "starting actor system"
  val system = ActorSystem.create(Keywords.APP,getActorConfig(app_context))

  protected[dag] def getActorConfig(app_context: AppContext): Config = {
    val actor_config =
      s"""
         | ${Keywords.ActorSys.CUSTOM_DISPATCHER} {
         |  type = Dispatcher
         |  executor = "thread-pool-executor"
         |  thread-pool-executor {
         |    core-pool-size-min = 3
         |    core-pool-size-factor = 3.0
         |    core-pool-size-max = ${ math.ceil(app_context.dagSetting.concurrency * 1.5) }
          |  }
          |  throughput = 1
          |}
          |
      """.stripMargin

    ConfigFactory parseString actor_config
  }

  def createWorker(dispatcher: String = "akka.actor.default-dispatcher") = {
    AppLogger debug s"creating worker pool with ${app_context.dagSetting.concurrency} worker(s)"
     system.actorOf(BalancingPool(app_context.dagSetting.concurrency).props(Props[Worker])
      .withDispatcher(dispatcher), "router")
  }

  def createPlayer(dag: Dag, workers: ActorRef ): ActorRef = {
    system.actorOf(Props(new DagPlayer(dag,app_context,workers)),"supervisor")
  }

}
