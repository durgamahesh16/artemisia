package tech.artemisia.dag

/**
  * Created by chlr on 8/20/16.
  */
object TestUtils {


  def worklet_file_import(file: String) =
    s"""
       |
       |task1 = {
       |  Component = TestComponent
       |  Task = TestAdderTask
       |  params = {
       |    num1 = 1
       |    num2 = 2
       |    result_var = tango1
       |  }
       |}
       |
       |task2 = {
       |  Component = DagEditor
       |  Task = Import
       |  dependencies = [task1]
       |  params = {
       |    file = $file
       |  }
       |}
       |
       |task3 = {
       |  Component = TestComponent
       |  Task = TestAdderTask
       |  dependencies = [task2]
       |  params = {
       |    num1 = $${bravo}
       |    num2 = 2
       |    result_var = tango2
       |  }
       |}
       |
       |
       |
     """.stripMargin

}
