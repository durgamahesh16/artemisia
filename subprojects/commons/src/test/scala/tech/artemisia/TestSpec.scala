package tech.artemisia

import org.scalatest._


/**
 * Created by chlr on 1/2/16.
 */

abstract class TestSpec extends FlatSpec with MustMatchers with PrePostTestSetup with OneInstancePerTest


trait PrePostTestSetup extends BeforeAndAfterEach {

  self: Suite =>

  /**
   * any pre-test code setup goes here
   */
  abstract override def beforeEach(): Unit = {
  }

  /**
   * Any post test clean code goes here
   */
  abstract override def afterEach(): Unit = {

  }

}