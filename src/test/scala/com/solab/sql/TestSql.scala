package com.solab.sql

import org.specs2.SpecificationWithJUnit
import org.apache.commons.dbcp.BasicDataSource
import org.specs2.specification.Step
import java.util.Date
import java.sql.{Time, Timestamp, Connection}

/** Tests for the Sql component.
 *
 * @author Enrique Zamudio
 * Date: 27/12/11 18:32
 */
class TestSql extends SpecificationWithJUnit { def is =

  "Test all the methods in Sql component" ^ Step(setup())             ^
  "Insert a simple row"                   ! insertSimpleRow           ^
  "Insert a row and get generated key"    ! insertRowWithGeneratedKey ^
  "Update existing row"                   ! updateExistingRow         ^
  "Update no rows"                        ! updateNothing             ^
  "Delete no rows"                        ! deleteNothing             ^
  "Delete a row"                          ! deleteOneRow              ^
  "Delete several rows"                   ! deleteManyRows            ^
                                            Step(insertTestRows())     ^
  "Query first row"                       ! queryFirstRow             ^
  "Query boolean"                         ! queryBoolean              ^
  "Query decimal"                         ! queryDecimal              ^
  "Query Int"                             ! queryInt                  ^
  "Query Long"                            ! queryLong                 ^
  "Query String"                          ! queryString               ^
  "Query other value"                     ! queryValue                ^
  "Query several rows"                    ! success ^
  "Query limited rows"                    ! pending ^
  "Query raw rows"                        ! success ^
  "Transaction with rollback"             ! success ^
  "Transaction with commit"               ! success ^
                                          Step(shutdown()) ^
                                          end

  var ds:BasicDataSource=_
  var testRowKey1:Long=0
  var testRowKey2:Long=0
  lazy val sql = new Sql(ds)

  def insertSimpleRow()={
    sql.executeUpdate("INSERT INTO scala_sql_test2 VALUES(?, ?, ?)", 1, "test1", 1) must be equalTo(1)
  }
  def insertRowWithGeneratedKey()={
    val keys = sql.executeInsert("INSERT INTO scala_sql_test1 (string, date, tstamp, colint,coldec) VALUES (?, ?, ?, ?, ?)",
      "test2", new Date, new Timestamp(1325089723956L), 2, BigDecimal(3))
    (keys.size must be equalTo(1)) and (keys(0).size must be equalTo(1)) and (keys(0)(0).asInstanceOf[Long] must be greaterThan(0))
  }
  def updateExistingRow()={
    val keys = sql.executeInsert("INSERT INTO scala_sql_test1 (string, date, tstamp, colint,coldec) VALUES (?, ?, ?, ?, ?)",
      "test3", new Date, new Timestamp(1325089723956L), 3, BigDecimal(4))
    val count = sql.executeUpdate("UPDATE scala_sql_test1 SET colbit=true WHERE pkey=?", keys(0)(0))
    val flag = sql.queryForBoolean("SELECT colbit FROM scala_sql_test1 WHERE pkey=?", keys(0)(0))
    (count must be equalTo(1)) and (flag must beSome[Boolean]) and (flag.get must beTrue)
  }
  def updateNothing()={
    sql.executeUpdate("UPDATE scala_sql_test1 SET colbit=true WHERE pkey=?", -1000) must be equalTo(0)
  }
  def deleteNothing()={
    sql.executeUpdate("DELETE FROM scala_sql_test1 WHERE pkey=?", -1000) must be equalTo(0)
  }
  def deleteOneRow()={
    val keys = sql.executeInsert("INSERT INTO scala_sql_test1 (string, date, tstamp, colint,coldec) VALUES (?, ?, ?, ?, ?)",
      "test3", new Date, new Timestamp(1325089723956L), 3, BigDecimal(4))
    sql.executeUpdate("DELETE FROM scala_sql_test1 WHERE pkey=?", keys(0)(0)) must be equalTo(1)
  }
  def deleteManyRows()={
    sql.executeInsert("INSERT INTO scala_sql_test1 (string, date, tstamp, colint,coldec) VALUES (?, ?, ?, ?, ?)",
      "test4", new Date, new Timestamp(2168867890L), 4, BigDecimal(5))
    sql.executeInsert("INSERT INTO scala_sql_test1 (string, date, tstamp, colint,coldec) VALUES (?, ?, ?, ?, ?)",
      "test5", new Date, new Timestamp(2168867890L), 5, BigDecimal(6))
    sql.executeInsert("INSERT INTO scala_sql_test1 (string, date, tstamp, colint,coldec) VALUES (?, ?, ?, ?, ?)",
      "test6", new Date, new Timestamp(2168867890L), 6, BigDecimal(7))
    sql.executeUpdate("DELETE FROM scala_sql_test1 WHERE tstamp=?", new Timestamp(2168867890L)) must be equalTo(3)
  }
  def queryFirstRow()={
    val existing = sql.firstRow("SELECT * FROM scala_sql_test1 WHERE pkey=?", testRowKey1)
    val absent   = sql.firstRow("SELECT * FROM scala_sql_test1 WHERE pkey=?", -5000)
    println("la tupla encontrada fue ", existing)
    (absent must beNone) and (existing must beSome[Map[String, Any]]) and (existing.get("pkey") must be equalTo(testRowKey1))
  }
  def queryBoolean()={
    val v1 = sql.queryForBoolean("SELECT colbit FROM scala_sql_test1 WHERE pkey=?", testRowKey1)
    sql.executeUpdate("UPDATE scala_sql_test1 SET colbit=NULL WHERE pkey=?", testRowKey1)
    val v2 = sql.queryForBoolean("SELECT colbit FROM scala_sql_test1 WHERE pkey=?", testRowKey1)
    (v1 must beSome[Boolean]) and (v1.get must beFalse) and (v2 must beNone)
  }
  def queryDecimal()={
    val v1 = sql.queryForDecimal("SELECT coldec FROM scala_sql_test1 WHERE pkey=?", testRowKey1)
    sql.executeUpdate("UPDATE scala_sql_test1 SET coldec=NULL WHERE pkey=?", testRowKey1)
    val v2 = sql.queryForDecimal("SELECT coldec FROM scala_sql_test1 WHERE pkey=?", testRowKey1)
    (v1 must beSome[BigDecimal]) and (v1.get must be equalTo(BigDecimal(200))) and (v2 must beNone)
  }
  def queryInt()={
    val v1 = sql.queryForInt("SELECT colint FROM scala_sql_test1 WHERE pkey=?", testRowKey1)
    sql.executeUpdate("UPDATE scala_sql_test1 SET colint=NULL WHERE pkey=?", testRowKey1)
    val v2 = sql.queryForInt("SELECT colint FROM scala_sql_test1 WHERE pkey=?", testRowKey1)
    (v1 must beSome[Int]) and (v1.get must be equalTo(100)) and (v2 must beNone)
  }
  def queryLong()={
    val v1 = sql.queryForLong("SELECT colint FROM scala_sql_test1 WHERE pkey=?", testRowKey2)
    sql.executeUpdate("UPDATE scala_sql_test1 SET colint=NULL WHERE pkey=?", testRowKey2)
    val v2 = sql.queryForLong("SELECT colint FROM scala_sql_test1 WHERE pkey=?", testRowKey2)
    (v1 must beSome[Long]) and (v1.get must be equalTo(100L)) and (v2 must beNone)
  }
  def queryString()={
    val v1 = sql.queryForString("SELECT string FROM scala_sql_test1 WHERE pkey=?", testRowKey1)
    sql.executeUpdate("UPDATE scala_sql_test1 SET string=NULL WHERE pkey=?", testRowKey1)
    val v2 = sql.queryForString("SELECT string FROM scala_sql_test1 WHERE pkey=?", testRowKey1)
    (v1 must beSome[String]) and (v1.get must be equalTo("test_row")) and (v2 must beNone)
  }
  def queryValue()={
    val v1 = sql.queryForValue[Timestamp]("SELECT tstamp FROM scala_sql_test1 WHERE pkey=?", testRowKey1)
    sql.executeUpdate("UPDATE scala_sql_test1 SET tstamp=NULL WHERE pkey=?", testRowKey1)
    val v2 = sql.queryForValue[Timestamp]("SELECT tstamp FROM scala_sql_test1 WHERE pkey=?", testRowKey1)
    (v1 must beSome[Timestamp]) and (v1.get.getTime must be equalTo(2168856789L)) and (v2 must beNone)
  }

  def setup() {
    //Create a pooled datasource for a test database
    ds = new BasicDataSource
    ds.setDriverClassName("org.h2.Driver")
    ds.setUrl("jdbc:h2:mem:scala_sql_tests;MODE=PostgreSQL")
    ds.setDefaultAutoCommit(true)
    ds.setUsername("sa")
    ds.setPassword("")
    ds.setDefaultTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED)
    ds.setInitialSize(Runtime.getRuntime.availableProcessors)
    //Now create some tables
    sql.execute("""
    CREATE TABLE scala_sql_test1(
      pkey SERIAL PRIMARY KEY,
      string VARCHAR(40),
      date   DATE,
      time   TIME,
      tstamp TIMESTAMP,
      colint INTEGER,
      coldec NUMERIC(12,4),
      colbit BOOLEAN
    )""")
    sql.execute("""
    CREATE TABLE scala_sql_test2(
      pkey INTEGER PRIMARY KEY,
      string VARCHAR(200),
      colint INTEGER NOT NULL
    )""")
  }

  def insertTestRows() {
    val when = 2168856789L
    val k1 = sql.executeInsert("INSERT INTO scala_sql_test1(string, date, time, tstamp, colint, coldec, colbit) VALUES (?, ?, ?, ?, ?, ?, ?)",
      "test_row", new Date(when), new Time(when), new Timestamp(when), 100, BigDecimal(200), false)
    val k2 = sql.executeInsert("INSERT INTO scala_sql_test1(string, date, time, tstamp, colint, coldec, colbit) VALUES (?, ?, ?, ?, ?, ?, ?)",
      "test_row", new Date(when), new Time(when), new Timestamp(when), 100, BigDecimal(200), false)
    testRowKey1 = k1(0)(0).asInstanceOf[Long]
    testRowKey2 = k2(0)(0).asInstanceOf[Long]
  }

  def shutdown() {
    ds.close()
  }

}
