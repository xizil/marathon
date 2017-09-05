package mesosphere.marathon
package core.task.update.impl.steps

import javax.inject.Inject

import akka.Done
import akka.actor.ActorSystem
import mesosphere.marathon.core.instance.update.{ InstanceChange, InstanceChangeHandler }

import scala.concurrent.Future

/**
  * Forward the update to the taskStatusEmitter.
  */
class TaskStatusEmitterPublishStepImpl @Inject() (system: ActorSystem) extends InstanceChangeHandler {

  override def name: String = "emitUpdate"

  override def process(update: InstanceChange): Future[Done] = {
    system.eventStream.publish(update)
    Future.successful(Done)
  }
}
