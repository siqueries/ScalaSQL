===========================================
ScalaSQL: A simple JDBC interface for Scala
===========================================

ScalaSQL is basically a component to handle simple interactions with databases via JDBC,
getting connections from a DataSource (if it's really necessary I'll add direct connection
support later on).

This is loosely based on the `Groovy SQL <http://groovy.codehaus.org/api/groovy/sql/Sql.html>`_ component, with some important differences:

- Scala doesn't have string interpolation (and the one that's coming in 2.10 sucks anyway), so
  only ``?``-type params are supported.
- Only DataSource support is available for now (direct connection support is really only useful
  for scripting, and sometimes even from a script you might want to handle transactions)
- The ``rows`` method returns an immutable list of immutable maps, following Scala's conventions
- The ``firstRow`` method returns an optional immutable map
- The ``eachRow`` method passes an immutable Map to the closure
- The maps returned by all these methods will have ``None`` as the value for NULL columns.
- The ``queryFor(Int|Long|String|BigDecimal|Date|Type)`` methods will all return Optional values.

The ``execute``, ``executeUpdate`` and ``executeInsert`` methods (should) work just like in Groovy.

I know there is already a `project similar to this one <https://github.com/johnbhurst/scalasql>`_, but
it doesn't have transaction support and it's got a bunch of stuff I don't really need (although, it has
already direct connection support, so if that's what you're looking for, it might be a better option for you).

I intend to use this for server-side applications, so thread safety and performance are very important
aspects of this project.
