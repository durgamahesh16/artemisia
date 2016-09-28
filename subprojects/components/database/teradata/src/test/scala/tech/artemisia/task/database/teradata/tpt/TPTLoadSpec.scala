package tech.artemisia.task.database.teradata.tpt

import tech.artemisia.TestSpec
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
  * Created by chlr on 9/28/16.
  */
class TPTLoadSpec extends TestSpec {

  "TPTLoad" must "properly monitor two futures when both succeeds" in {
    val fut1 = Future{()}
    val fut2 = Future{()}
    val resultFuture = TPTLoad.monitor(fut1, fut2)
    resultFuture.value.get must be (Success(() -> ()))
  }

  it must "properly monitor two futures when one fails" in {
    val ex = new RuntimeException("dummy")
    val fut1 = Future.failed(ex)
    val fut2 = Future{()}
    val resultFuture = TPTLoad.monitor(fut1, fut2)
    resultFuture.value.get must be (Failure(ex))
    resultFuture onComplete {
      case Success(x) => fail("the future should have failed")
      case Failure(x) => x.getMessage must be ("dummy")
    }
  }

}
