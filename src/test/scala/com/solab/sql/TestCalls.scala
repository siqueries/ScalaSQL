package com.solab.sql

import org.specs2.SpecificationWithJUnit
import org.specs2.specification.Step
import org.apache.commons.dbcp.BasicDataSource
import java.sql.Connection

/** Acceptance tests for the callable statement functionality.
 *
 * @author Enrique Zamudio
 * Date: 06/01/12 15:29
 */
class TestCalls extends SpecificationWithJUnit { def is =

  "Register SP's in H2" ^ Step(registerFunctions()) ^
  "Simple call"          ! simpleCall()             ^
  "Call out no params"   ! callOutNoParams()        ^
  "Call out with params" ! callOutParams()          ^
                           Step(shutdown())         ^
                           end

  var ds:BasicDataSource=_
  lazy val sql = new Sql(ds)

  def registerFunctions() {
    ds = new BasicDataSource
    ds.setDriverClassName("org.h2.Driver")
    ds.setUrl("jdbc:h2:mem:scala_sql_tests;MODE=PostgreSQL")
    ds.setDefaultAutoCommit(true)
    ds.setUsername("sa")
    ds.setPassword("")
    ds.setDefaultTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED)
    ds.setInitialSize(Runtime.getRuntime.availableProcessors)
    sql.execute("""
      CREATE ALIAS test_function1 FOR "com.solab.sql.H2Functions.function1";
      CREATE ALIAS test_function2 FOR "com.solab.sql.H2Functions.function2";
    """)
  }

  def shutdown() {
    ds.close()
  }

  def simpleCall()={
    sql.callUpdate("{call test_function1()}") must be equalTo(0)
  }
  def callOutNoParams()={
    sql.call("{?=call test_function1()}", OutParams.VARCHAR) must be equalTo(Map(1->"This is function1"))
  }
  def callOutParams()={
    sql.call("{?=call test_function2(?, ?)}", OutParams.DECIMAL, "String", BigDecimal(1.1)) must be equalTo(Map(1->BigDecimal(2.1)))
  }

}
