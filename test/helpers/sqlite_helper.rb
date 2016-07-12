puts "configuring jdbc driver for sqlite..."

require_relative 'sqlite-jdbc-3.8.11.2.jar'
java_import 'org.sqlite.JDBC'