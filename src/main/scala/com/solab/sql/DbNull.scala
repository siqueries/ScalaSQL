package com.solab.sql

/** Represents a NULL value that is sent to the database, along with its SQL type.
 *
 * @author Enrique Zamudio
 * Date: 06/01/12 14:46
 */
class DbNull(val sqlType:Int)

object DbNull {
  /** Creates a new DbNull with the specified SQL type (from java.sql.Types). */
  def apply(sqlType:Int) = new DbNull(sqlType)
}
