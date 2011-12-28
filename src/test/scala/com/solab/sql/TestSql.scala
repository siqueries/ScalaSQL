package com.solab.sql

import org.specs2.SpecificationWithJUnit
import org.apache.commons.dbcp.BasicDataSource
import java.sql.Connection
import org.specs2.specification.Step

/** Tests for the Sql component.
 *
 * @author Enrique Zamudio
 * Date: 27/12/11 18:32
 */
class TestSql extends SpecificationWithJUnit { def is =

  "Test all the methods in Sql component" ^ Step(setup()) ^
  "Insert a simple row" ! success ^
  "Insert a row and get generated key" ! success ^
  "Update existing row" ! success ^
  "Update no rows" ! success ^
  "Delete no rows" ! success ^
  "Delete a row" ! success ^
  "Query first row" ! success ^
  "Query boolean" ! success ^
  "Query decimal" ! success ^
  "Query Int" ! success ^
  "Query Long" ! success ^
  "Query String" ! success ^
  "Query other value" ! success ^
  "Query several rows" ! success ^
  "Query limited rows" ! success ^
  "Query raw rows" ! success ^
  "Transaction with rollback" ! success ^
  "Transaction with commit" ! success ^
  Step(shutdown()) ^
  end

  def setup() {
    if (Aux.sql == null) {
      val ds = new BasicDataSource
      ds.setDriverClassName("org.h2.Driver")
      ds.setUrl("jdbc:h2:mem:scala_sql_tests;MODE=PostgreSQL")
      ds.setDefaultAutoCommit(true)
      ds.setUsername("sa")
      ds.setPassword("")
      ds.setDefaultTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED)
      ds.setInitialSize(Runtime.getRuntime.availableProcessors)
      Aux.ds=ds
      Aux.sql=new Sql(ds)
    }
  }

  def shutdown() {
    Aux.ds.close()
  }

}

object Aux {
  var ds:BasicDataSource=_
  var sql:Sql=_
}
