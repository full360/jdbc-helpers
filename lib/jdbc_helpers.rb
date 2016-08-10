require 'json'
require 'logger'

# JDBC helper classes to simplify interactions with JDBC databases.
# note that this assumes that you have already instantiated
# a JDBC driver into the current namespace.
module JDBCHelpers
  # Base class to provide helper methods that may be generally useful to the concrete classes
  class Base
    # Cleanses SQL statements so they can be presented in a log without security risk
    # currently removes AWS credentials for Redshift copy/unload
    # @param [String] stmt SQL statement to cleanse
    # @return [String] cleansed statement
    def cleanse_statement(stmt)
      # filter out aws creds from redshift
      tmp = stmt.to_s
      return tmp.gsub(
        /aws_access_key_id\s*=\s*.{20}\s*\;\s*|aws_secret_access_key\s*=\s*(\w|\/|^'){40,41}/i,
        '<removed>'
      ).gsub(/\s+/,' ')
    end
    
    # Array of classes that should be casted to Strings
    # @return [Array<Class>]
    def convert_to_string_classes
      [Java::JavaSql::Timestamp, Java::JavaSql::Date]
    end
  end

  # Creates a discreet JDBC connection and makes it available via connection attribute
  class ConnectionFactory < JDBCHelpers::Base
    attr_accessor :connection
    
    # @param [String] jdbc_url
    # @param [String] db_user
    # @param [String] db_pass
    # @param [Logger] logger object otherwise will default to new Logger 
    def initialize(jdbc_url, db_user, db_pass, logger = nil)
      @logger = logger ? logger : Logger.new(STDOUT)
      @logger.info("connecting to #{jdbc_url} as user #{db_user}...")
      @connection = java.sql.DriverManager.get_connection(
        jdbc_url,
        db_user,
        db_pass
      )
      @logger.info("connection successful!") if @connection
    end
  end

  # executes a sql statement
  class Execute < JDBCHelpers::Base
    # Rows affected by the SQL statement execution. Note that
    # not all JDBC drivers/databases handle this appropriately
    attr_accessor :rows_affected

    # @param [Object] db_connect active connection against which to execute statement
    # @param [String] statement SQL statement text
    # @param [Logger] logger object otherwise will default to new Logger 
    def initialize(db_connect, statement, logger = nil)
      @logger = logger ? logger : Logger.new(STDOUT)
      stmt = db_connect.create_statement
      @logger.info(
        "executing statement: #{cleanse_statement(statement)}"
      )
      start = Time.new.utc
      @rows_affected = stmt.execute_update(statement)
      @logger.info "query executed #{Time.new.utc - start} seconds"
    end
  end

  # Executes a select query, then returns the first field from the first row
  class SingleValueFromQuery < JDBCHelpers::Base
    # Value of the first field from the first row. class will vary.
    attr_accessor :result
    
    # @param [Object] db_connect active connection against which to execute statement
    # @param [String] statement SQL statement text
    # @param [Logger] logger object otherwise will default to new Logger 
    def initialize(db_connect, statement, logger = nil)
      @logger = logger ? logger : Logger.new(STDOUT)
      stmt = db_connect.create_statement
      @logger.info(
        "executing query: #{cleanse_statement(statement)}"
      )
      start = Time.new.utc
      rs = stmt.execute_query(statement)
      @logger.info "query executed #{Time.new.utc-start} seconds"
      rs.next
      value = rs.getObject(1)
      rs.close

      #the below simplifies things... especially comparisons
      #if you don't like it you can write your own damn helper!
      value = value.to_s if convert_to_string_classes.include?(value.class)
      @result = value
    end
  end

  # Execute a SQL query, store the results as an array of hashes.
  class QueryResultsToArray < JDBCHelpers::Base
    # Contains the array of hashes returned from the select query
    attr_accessor :results
  
    # @param [Object] db_connect active connection against which to execute statement
    # @param [String] statement SQL statement text
    # @param [Logger] logger object otherwise will default to new Logger 
    def initialize(db_connect, statement, logger = nil)
      @logger = logger ? logger : Logger.new(STDOUT)
      stmt = db_connect.create_statement
      @logger.info "executing query: #{cleanse_statement(statement)}"
      start = Time.new.utc
      rs = stmt.execute_query(statement)
      @logger.info("query executed #{Time.new.utc - start} seconds")
      @results = rs_to_array(rs)
    end

    # converts a JDBC recordset to an array of hashes, with one hash per record
    # @param [Object] rs JDBC result set object
    # @return [Array<Hash>] Array of Hash with a Hash for each record
    def rs_to_array(rs)
      # creates an array of hashes from a jdbc record set
      arr = []

      # get basic metadata for the recordset
      meta = rs.getMetaData
      cols = meta.getColumnCount.to_i
      
      puts meta.public_methods.sort

      # loop through the records to add them into hash
      while rs.next do
        # r is a temporary hash for the row being processed
        r = {}

        # add each row to r
        (1..cols).each do |c|
          r[meta.get_column_name(c)] = rs.getObject(c)
          if convert_to_string_classes.include?(r[meta.get_column_name(c)].class)
            r[meta.get_column_name(c)] = r[meta.get_column_name(c)].to_s
          end
        end # each cols

        # append hash to array
        arr << r
      end # while

      # completed hash is returned
      return arr
    end
  end

  # Creates a hash of arrays of hashes from a SQL query
  class QueryResultsToHashOfArrays < JDBCHelpers::Base
    attr_accessor :results

    # @param [Object] db_connect active connection against which to execute statement
    # @param [String] statement SQL statement text
    # @param [String] key_field SQL result set field containing the key to be used in the top level of hash
    # @param [Logger] logger object otherwise will default to new Logger 
    def initialize(db_connect, statement, key_field, logger = nil)
      @logger = logger ? logger : Logger.new(STDOUT)
      stmt = db_connect.create_statement
      @logger.info "executing query in thread #{Thread.current.object_id}:\n#{cleanse_statement(statement)}"
      start = Time.new.utc
      rs = stmt.execute_query(statement)
      @logger.info "query executed #{Time.new.utc - start} seconds"
      @results=rs_to_hash(rs, key_field, true)
    end

    # converts a JDBC recordset to an array of hashes, with one hash per record
    # creates a hash from a jdbc record set
    # index_key_field is the field you want to use as the top level
    # hash key... and should exist in the record set
    # multi_val=true will create an array below each index_key_filed,
    # false will create a hash as the child
    # @param [Object] rs JDBC result set object
    # @param [String] index_key_field field to use as top level hash keys
    # @return [Hash] Hash of Arrays of Hashes for each record
    def rs_to_hash(rs, index_key_field, multi_val)
      # setting default hash value is necessary for appending to arrays
      hash=Hash.new{ |h, k| h[k] = [] }

      # get basic metadata for the recordset
      meta = rs.getMetaData
      cols = meta.getColumnCount.to_i

      # loop through the records to add them into hash
      while rs.next do
        # if multi_val is not true... create new hash value as an empty hash if it doesn't already exist
        hash[rs.getString(index_key_field)]={} if (!hash[rs.getString(index_key_field)] and !multi_val)

        # if multi_val is true... create new hash value as an empty array if it doesn't already exist
        hash[rs.getString(index_key_field)]=[] if (!hash[rs.getString(index_key_field)] and multi_val)

        # r is a temporary hash for the row being processed
        r=Hash.new

        # add each row to r
        (1..cols).each do |c|
          r[meta.get_column_name(c)] = rs.getObject(c)
          if convert_to_string_classes.include?(r[meta.get_column_name(c)].class)
            r[meta.get_column_name(c)] = r[meta.get_column_name(c)].to_s
          end
        end # each cols

        # set hash value to r if not multi_val
        hash[rs.getString(index_key_field)] = r if !multi_val

        # append hash to r if multi_val
        hash[rs.getString(index_key_field)] << r if multi_val
      end # while

      # completed hash is returned
      return hash
    end
  end # class

  class QueryResultsToJSONFile < JDBCHelpers::Base
    # runs a SQL query, then writes the results as JSON objects to a provided file object.
    # by use of the formatter parameter, you can change the output to any format you desire (CSV, XML, etc)
    # see json_formatter method for an example of a proc to perform formatting
    # @param [Object] db_connect active connection against which to execute statement
    # @param [String] statement SQL statement text
    # @param [IO] file_object IO object to receive the formatted results
    # @param [proc] formatter proc to handle the actual formatting, defaults to JSON if nil
    # @param [Logger] logger object otherwise will default to new Logger 
    def initialize(db_connect, statement, file_object, formatter = nil, logger = nil)
      @logger = logger ? logger : Logger.new(STDOUT)
      stmt = db_connect.create_statement
      @logger.info "executing query: #{cleanse_statement(statement)}"
      start = Time.new.utc
      rs = stmt.execute_query(statement)
      @logger.info "query executed #{Time.new.utc - start} seconds"
      rs_to_json_file(rs, file_object, formatter)
    end

    # outputs a JDBC result set to a formatted file
    # formatter defaults to JSON output unless you provide your own proc
    # @param [Object] rs JDBC result set
    # @param [IO] file_object IO object to receive the formatted results
    # @param [proc] formatter proc to handle the actual formatting, defaults to JSON if nil
    def rs_to_json_file(rs, file_object, formatter)
      # default formatter outputs json objects for each row
      formatter = json_formatter unless formatter

      # get basic metadata for the recordset
      meta = rs.getMetaData
      cols = meta.getColumnCount.to_i

      # loop through the records to add them into hash
      while rs.next do

        # r is a temporary hash for the row being processed
        r = Hash.new

        # add each row to r
        (1..cols).each do |c|
          r[meta.get_column_name(c)] = rs.getObject(c)
          if convert_to_string_classes.include?(r[meta.get_column_name(c)].class)
            r[meta.get_column_name(c)] = r[meta.get_column_name(c)].to_s
          end
        end # each cols

        # formatter handles output of r to file
        formatter.call(file_object, r)
      end # while
    end
    
    # proc must handle two inputs, |file_object, record hash|
    # @return [proc] returns proc to output json
    def json_formatter
      Proc.new { |f,h| f.puts h.to_json }
    end
  end # class
end
