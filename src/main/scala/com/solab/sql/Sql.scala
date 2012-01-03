/*
 * Copyright 2011 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.solab.sql

import javax.sql.DataSource
import java.io.{Reader, InputStream}
import java.sql._

/** A class similar to groovy.sql.Sql but with only DataSource support for the moment.
 *
 * The connections are thread-bound, which allows for the query and update methods to be called
 * from the withTransaction method, which causes the same connection to be used instead of separate
 * ones.
 *
 * @param dataSource A DataSource from which to get connections from.
 *
 * @author Enrique Zamudio
 * Date: 09/12/11 14:02
 */
class Sql(val dataSource:DataSource) {

  val conns = new ThreadLocalConnection(dataSource)

  /** Sets the parameters of the statement, according to their type. */
  def prepareStatement(stmt:PreparedStatement, params:Any*):PreparedStatement={
    var count=1
    params.foreach { p =>
      p match {
        case x:Int => stmt.setInt(count, x)
        case x:Long => stmt.setLong(count, x)
        case x:Short => stmt.setShort(count, x)
        case x:Byte => stmt.setByte(count, x)
        case x:Boolean => stmt.setBoolean(count, x)
        case x:BigDecimal => stmt.setBigDecimal(count, x.bigDecimal)
        case x:String => stmt.setString(count, x)
        case x:Timestamp => stmt.setTimestamp(count, x)
        case x:java.sql.Date => stmt.setDate(count, x)
        case x:java.util.Date => stmt.setDate(count, new java.sql.Date(x.getTime))
        case x:Double => stmt.setDouble(count, x)
        case x:Float => stmt.setFloat(count, x)
        case x:scala.Array[Byte] => stmt.setBytes(count, x)
        case x:Reader => stmt.setClob(count, x)
        case x:InputStream => stmt.setBlob(count, x)
        case x => stmt.setObject(count, x)
      }
      count+=1
    }
    stmt
  }

  /** Creates a PreparedStatement for the specified connection, with the specified SQL and parameters.
   * @param conn The connection from which to create the PreparedStatement.
   * @param sql An SQL statement.
   * @param params the parameters for the SQL statement. */
  def prepareStatement(conn:Connection, sql:String, params:Any*):PreparedStatement={
    prepareStatement(conn.prepareStatement(sql), params:_*)
  }

  /** Creates a PreparedStatement from the specified connection, with the specified SQL and parameters,
   * configured to return the generated keys resulting from executing an insert statement.
   * @param conn The connection from which to create the PreparedStatement.
   * @param sql An insert statement.
   * @param params The parameters for the insert statement. */
  def prepareInsertStatement(conn:Connection, sql:String, params:Any*):PreparedStatement={
    val stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)
    prepareStatement(stmt, params:_*)
  }

  /** Executes a parameterized SQL statement, using a connection from the DataSource. */
  def execute(sql:String, params:Any*):Boolean={
    val conn = conns.get()
    var rval=false
    try {
      val stmt = prepareStatement(conn.connection(), sql, params:_*)
      try {
        rval=stmt.execute()
      } finally stmt.close()
    } finally conn.close()
    rval
  }
  /** Executes a simple SQL statement, using a connection from the DataSource. */
  def execute(sql:String):Boolean={
    val conn = conns.get()
    var rval=false
    try {
      val stmt = conn.connection().createStatement()
      try {
        rval=stmt.execute(sql)
      } finally stmt.close()
    } finally conn.close()
    rval
  }

  /** Executes the specified SQL as an update statement
   * @param sql The statement to execute
   * @param params The parameters for the SQL statement.
   * @return The number of rows that were updated (the result of calling executeUpdate on the PreparedStatement). */
  def executeUpdate(sql:String, params:Any*):Int={
    val conn = conns.get()
    var rval= -1
    try {
      val stmt = prepareStatement(conn.connection(), sql, params:_*)
      try {
        rval=stmt.executeUpdate()
      } finally stmt.close()
    } finally conn.close()
    rval
  }

  /** Executes an insert statement. The difference with executeUpdate is that this method returns the generated keys
   * resulting from the inserted rows, if any.
   * @param sql The insert statement.
   * @param params The parameters for the insert statement.
   * @return The generated keys, if any. */
  def executeInsert(sql:String, params:Any*):IndexedSeq[IndexedSeq[Any]]={
    val conn = conns.get()
    try {
      val stmt = prepareStatement(conn.connection(), sql, params:_*)
      try {
        val count = 1 to stmt.executeUpdate()
        val rs = stmt.getGeneratedKeys
        try {
          val rango = 1 to rs.getMetaData.getColumnCount
          count.map { rowIndex =>
            rs.next()
            rango.map { rs.getObject(_) }
          }
        } finally rs.close()
      } finally stmt.close()
    } finally conn.close()
  }

  /** Executes a parameterized query and calls a function with each row, passing it the ResultSet. The function
   * does not need to call next().
   * @param sql The query to run.
   * @param params The parameters to pass to the query.
   * @param body The function to call for each row. It is passed the ResultSet but it doesn't need to call next() on it
   * since this is done inside this method. */
  def eachRawRow(sql:String, params:Any*)(body: ResultSet => Unit) {
    val conn = conns.get()
    try {
      val stmt = prepareStatement(conn.connection(), sql, params:_*)
      try {
        val rs = stmt.executeQuery()
        try {
          while (rs.next()) {
            body(rs)
          }
        } finally rs.close()
      } finally stmt.close()
    } finally conn.close()
  }

  /** Runs a parameterized query and calls a function with each row, passing it the ResultSet. The function
   * does not need to call next().
   * @param sql The query to run.
   * @param limit The maximum number of rows to process (-1 to return ALL rows)
   * @param offset The number of rows to skip before starting to process results
   * @param params The parameters to pass to the query.
   * @param body A function to be called for each row, taking the ResultSet as parameter. It doesn't need to call
   * next() since it is called from within this method.
   */
  def eachRawRow(limit:Int, offset:Int, sql:String, params:Any*)(body: ResultSet => Unit) {
    val conn = conns.get()
    try {
      val stmt = prepareStatement(conn.connection(), sql, params:_*)
      if (limit > 0 && offset <= 0) stmt.setMaxRows(limit)
      try {
        val rs = stmt.executeQuery()
        try {
          //This is to determine if we can continue after skipping offset rows
          val cont =
            if (offset > 0) 1 to offset exists { x => !rs.next() }
            else true
          if (limit > 0) {
            var count = 0
            //Read up to limit rows
            while (count < limit && rs.next()) {
              body(rs)
              count+=1
            }
          } else {
            //Read everything; limit is either already set or ignored
            while (cont && rs.next()) {
              body(rs)
            }
          }
        } finally rs.close()
      } finally stmt.close()
    } finally conn.close()
  }

  /** Runs a parameterized query and calls a function with each row, passing the row as a Map.
   * @param sql The query to run.
   * @param params The parameters to pass to the query.
   * @param body A function to be called for each row, taking a Map[String,Any] as parameter. The keys
   * will be the column names, in lowercase.
   */
  def eachRow(sql:String, params:Any*)(body: Map[String,Any] => Unit) {
    val conn = conns.get()
    try {
      val stmt = prepareStatement(conn.connection(), sql, params:_*)
      try {
        val rs = stmt.executeQuery()
        try {
          val meta = rs.getMetaData
          val range = 1 to meta.getColumnCount
          while (rs.next()) {
            val row = range.map { mapColumn(rs, meta, _) }.toMap
            body(row)
          }
        } finally rs.close()
      } finally stmt.close()
    } finally conn.close()
  }

  /** Returns a List of all the rows returned by the query. Each row is a Map with the column names as keys, in lowercase. */
  def rows(sql:String, params:Any*):List[Map[String, Any]]={
    var rows:List[Map[String, Any]] = Nil
    eachRow(sql, params:_*) { m =>
      rows = rows:+m
    }
    rows
  }

  /** Runs a parameterized query and calls a function with each row, passing the row as a Map.
   * @param sql The query to run.
   * @param limit The maximum number of rows to process (-1 to return ALL rows)
   * @param offset The number of rows to skip before starting to process results
   * @param params The parameters to pass to the query.
   * @param body A function to be called for each row, taking a Map[String,Any] as parameter. The keys
   * will be the column names, in lowercase.
   */
  def eachRow(limit:Int, offset:Int, sql:String, params:Any*)(body: Map[String,Any] => Unit) {
    val conn = conns.get()
    try {
      val stmt = prepareStatement(conn.connection(), sql, params:_*)
      if (limit > 0 && offset <= 0) stmt.setMaxRows(limit)
      try {
        val rs = stmt.executeQuery()
        try {
          val meta = rs.getMetaData
          val range = 1 to meta.getColumnCount
          //This is to determine if we can continue after skipping offset rows
          val cont =
            if (offset > 0) 1 to offset exists { x => !rs.next() }
            else true
          if (limit > 0) {
            var count = 0
            //Read up to limit rows
            while (count < limit && rs.next()) {
              val row = range.map { mapColumn(rs, meta, _) }.toMap
              body(row)
              count+=1
            }
          } else {
            //Read everything; limit is either already set or ignored
            while (cont && rs.next()) {
              val row = range.map { mapColumn(rs, meta, _) }.toMap
              body(row)
            }
          }
        } finally rs.close()
      } finally stmt.close()
    } finally conn.close()
  }

  /** Runs the query and returns a List with no more than `limit` rows, and skipping `offset` rows before collecting the
   * results.
   * @param sql The parameterized query to run
   * @param limit The maximum number of rows to return
   * @param offset The number of rows to skip before starting to collect the resulting rows
   * @param params The parameters for the query */
  def rows(limit:Int, offset:Int, sql:String, params:Any*):List[Map[String, Any]]={
    var rows:List[Map[String, Any]] = Nil
    eachRow(limit, offset, sql, params:_*) { m =>
      rows = rows:+m
    }
    rows
  }

  /** Returns the first row, if any, for the specified query.
   * @param sql The query string.
   * @param params The parameters for the query.
   * @return A Option[Map] representing the row if one was found, with the column names as keys, in all-lowercase. */
  def firstRow(sql:String, params:Any*):Option[Map[String, Any]]={
    val conn = conns.get()
    try {
      val stmt = prepareStatement(conn.connection(), sql, params:_*)
      stmt.setMaxRows(1)
      try {
        val rs = stmt.executeQuery()
        try {
          if (rs.next()) {
            val meta = rs.getMetaData
            Some((1 to meta.getColumnCount).map { mapColumn(rs, meta, _) }.toMap)
          } else None
        } finally rs.close()
      } finally stmt.close()
    } finally conn.close()
  }

  /** Returns the first value of the first row of the query, as an optional Int. */
  def queryForInt(sql:String, params:Any*):Option[Int]={
    val conn = conns.get()
    try {
      val stmt = prepareStatement(conn.connection(), sql, params:_*)
      stmt.setMaxRows(1)
      try {
        val rs = stmt.executeQuery()
        try {
          if (rs.next()) {
            val v = rs.getInt(1)
            if (rs.wasNull()) None else Some(v)
          } else None
        } finally rs.close()
      } finally stmt.close()
    } finally conn.close()
  }
  /** Returns the first value of the first row of the query, as an optional Long. */
  def queryForLong(sql:String, params:Any*):Option[Long]={
    val conn = conns.get()
    try {
      val stmt = prepareStatement(conn.connection(), sql, params:_*)
      stmt.setMaxRows(1)
      try {
        val rs = stmt.executeQuery()
        try {
          if (rs.next()) {
            val v = rs.getLong(1)
            if (rs.wasNull()) None else Some(v)
          } else None
        } finally rs.close()
      } finally stmt.close()
    } finally conn.close()
  }
  /** Returns the first value of the first row of the query, as an optional BigDecimal. */
  def queryForDecimal(sql:String, params:Any*):Option[BigDecimal]={
    val conn = conns.get()
    try {
      val stmt = prepareStatement(conn.connection(), sql, params:_*)
      stmt.setMaxRows(1)
      try {
        val rs = stmt.executeQuery()
        try {
          if (rs.next()) {
            val v = rs.getBigDecimal(1)
            if (rs.wasNull()) None else Some(BigDecimal(v))
          } else None
        } finally rs.close()
      } finally stmt.close()
    } finally conn.close()
  }
  /** Returns the first value of the first row of the query, as an optional String. */
  def queryForString(sql:String, params:Any*):Option[String]={
    val conn = conns.get()
    try {
      val stmt = prepareStatement(conn.connection(), sql, params:_*)
      stmt.setMaxRows(1)
      try {
        val rs = stmt.executeQuery()
        try {
          if (rs.next()) {
            val v = rs.getString(1)
            if (rs.wasNull()) None else Some(v)
          } else None
        } finally rs.close()
      } finally stmt.close()
    } finally conn.close()
  }
  /** Returns the first value of the first row of the query, as an optional Boolean. */
  def queryForBoolean(sql:String, params:Any*):Option[Boolean]={
    val conn = conns.get()
    try {
      val stmt = prepareStatement(conn.connection(), sql, params:_*)
      stmt.setMaxRows(1)
      try {
        val rs = stmt.executeQuery()
        try {
          if (rs.next()) {
            val v = rs.getBoolean(1)
            if (rs.wasNull()) None else Some(v)
          } else None
        } finally rs.close()
      } finally stmt.close()
    } finally conn.close()
  }
  /** Returns the first value of the first row of the query, as an optional value of the specified type. */
  def queryForValue[T](sql:String, params:Any*):Option[T]={
    val conn = conns.get()
    try {
      val stmt = prepareStatement(conn.connection(), sql, params:_*)
      stmt.setMaxRows(1)
      try {
        val rs = stmt.executeQuery()
        try {
          if (rs.next()) {
            val v = rs.getObject(1).asInstanceOf[T]
            if (rs.wasNull()) None else Some(v)
          } else None
        } finally rs.close()
      } finally stmt.close()
    } finally conn.close()
  }

  /** Creates a connection, executes the function within an open transaction, and commits the transaction at the end,
   * or rolls back if an exception is thrown.
   * @param body The function to execute with the open transaction. It is passed the connection as an argument. */
  def withTransaction(body: ConnectionStatus => Unit) {
    val conn = conns.get()
    conn.beginTransaction()
    var ok = false
    try {
      body(conn)
      ok = true
    } finally {
      try {
        if (ok) conn.commit() else conn.rollback()
      } finally {
        conn.close()
      }
    }
  }

  /** Creates and returns a Tuple2 with the specified column's lowercase name and its value. If the value is null, the returned
   * value is None.
   * @param rs An open ResultSet
   * @param meta The ResultSet's metadata
   * @param idx The column index (starting at 1). */
  private def mapColumn(rs:ResultSet, meta:ResultSetMetaData, idx:Int):(String, Any)={
    val v = rs.getObject(idx)
    (meta.getColumnName(idx).toLowerCase -> (if (rs.wasNull()) None else v))
  }

}
