package tech.artemisia.task.database.teradata

import java.sql.SQLException
import javax.xml.bind.DatatypeConverter

import tech.artemisia.TestSpec
import tech.artemisia.task.TaskContext

/**
  * Created by chlr on 7/12/16.
  */
class FastLoadErrorRecordHandlerSpec extends TestSpec {

  "FastLoadErrorRecordHandler" must "parse Exceptions" in {
    val tableName = s"fastload_error_table"
    val errorHandler = new FastLoadErrorRecordHandler(tableName)
    val msg1 =
      s"""|Row 1 in FastLoad table "sandbox"."${tableName}_ERR_1" contains the following data:
         |ErrorCode=5317
         |ErrorFieldName=F_col1
         |ActualDataParcelLength=15
         |DataParcel: byte array length 15 (0xf), offset 0 (0x0), dump length 15 (0xf)
         |00000   00 00 00 00 3c 00 04 79 79 79 79 00 11 65 85     |....<..yyyy..e.| """.stripMargin
    val exception1 = new SQLException(msg1)
    errorHandler.parseException(exception1)

    val msg2 =
      s"""|[Teradata JDBC Driver] [TeraJDBC 15.10.00.14] [Error 1160] [SQLState HY000] Row 1 in FastLoad table "sandbox"."${tableName}_ERR_2" contains the following data:
         |col1=4
         |col2=xxxx
         |col3=2014-01-01""".stripMargin
    val exception2 = new SQLException(msg2)
    errorHandler.parseException(exception2)

    val file_et = TaskContext.getTaskFile("error_et.txt")
    val file_uv = TaskContext.getTaskFile("error_uv.txt")

    errorHandler.close()

    scala.io.Source.fromFile(file_et).mkString("") must be (s"F_col1,5317,${new String(DatatypeConverter.parseHexBinary("000000003c00047979797900116585"))}\n")
    scala.io.Source.fromFile(file_uv).mkString("") must be ("col1=4,col2=xxxx,col3=2014-01-01\n")

  }

}
