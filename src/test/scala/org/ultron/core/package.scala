package org.ultron

import org.ultron.task.TestComponent
import org.ultron.util.{OSUtil, OSUtilTestImpl}
import scaldi.{Injector, Module}

/**
 * Created by chlr on 1/1/16.
 */
package object core {
  implicit var wire: Injector = getWireObject

  def getWireObject = {
    new Module {
      bind[OSUtil] to new OSUtilTestImpl
      bind[ComponentDispatcher] identifiedBy "TestComponent" to new TestComponent
    }
  }
}
