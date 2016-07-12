##JDBC Helpers for JRuby

The classes in this library are intended to assist in interacting with JDBC databases when programming in JRuby.

This should work with any JDBC database, but it has only been tested with:

* Vertica
* Redshift
* SQLite


###Installation

```gem install jdbc-helpers```

###JDBC Drivers

Note that you will need to include a JDBC driver into the current namespace before these methods will work:

```ruby
require_relative 'sqlite-jdbc-3.8.11.2.jar'
java_import 'org.sqlite.JDBC'
```

From there you can use the classes in this library:

Create a connection object...
```ruby
conn = JDBCHelpers::ConnectionFactory.new('jdbc:sqlite:memory', '', '').connection
```

...then you can use the connection to do other things:

```ruby
JDBCHelpers::Execute.new(
  conn,
  "create table test(a integer, b varchar(15), c timestamp, d date);"
)

JDBCHelpers::Execute.new(
  conn,
  "insert into test values(12345,'chicken','2016-07-01T23:23:23.000','2016-07-01');"
)

JDBCHelpers::Execute.new(
  conn,
  "insert into test values(12346,'turkey','2016-07-01T23:23:23.000','2016-07-01');"
)
```

Note that you can use a connection object created without this gem if desired.