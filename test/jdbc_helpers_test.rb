gem "minitest"
require 'minitest/autorun'

# helpers to instantiate the JDBC driver for sqlite
require_relative "#{File.dirname(__FILE__)}/helpers/sqlite_helper.rb"

#import the gem library
require_relative "#{File.dirname(__FILE__)}/../lib/jdbc_helpers.rb"

class TestJDBCHelpers < Minitest::Test

  def give_me_a_simple_test_database
    File.delete('memory') if File.exists? 'memory'
    conn = JDBCHelpers::ConnectionFactory.new(
      'jdbc:sqlite:memory',
      '',
      ''
    ).connection
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
    return conn
  end
  
  def close_simple_test_database(conn)
    conn.close
    File.delete('memory') if File.exists?('memory')
  end
  
  def test_connection_factory
    #doesn't use give_me_a_simple_test_database but I'm not sure if that matters
    conn = JDBCHelpers::ConnectionFactory.new(
      'jdbc:sqlite:memory',
      '',
      ''
    ).connection
    assert_equal(conn.class, Java::OrgSqlite::SQLiteConnection)
    
    #clean up
    conn.close
    File.delete('memory')
  end
  
  def test_execute
    conn = give_me_a_simple_test_database

    #test by selecting the record and checking for the number 3
    assert_equal(
      JDBCHelpers::SingleValueFromQuery.new(conn,"select a from test where b='chicken';").result,
      12345
    )
    
    #clean up
    close_simple_test_database conn
  end  

  def test_single_value_from_query
    conn = give_me_a_simple_test_database
    
    # test by selecting the record and checking for the respective values
    assert_equal(JDBCHelpers::SingleValueFromQuery.new(conn,"select a from test where b='chicken';").result, 12345)
    assert_equal(JDBCHelpers::SingleValueFromQuery.new(conn,"select b from test where a=12345;").result, 'chicken')
    # these should be returned as strings
    assert_equal(JDBCHelpers::SingleValueFromQuery.new(conn,"select c from test where a=12345;").result, '2016-07-01T23:23:23.000')
    assert_equal(JDBCHelpers::SingleValueFromQuery.new(conn,"select d from test where a=12345;").result, '2016-07-01')
    # insure that large integers come back np
    assert_equal(JDBCHelpers::SingleValueFromQuery.new(conn,"select 80000000000;").result, 80000000000)
    # clean up
    close_simple_test_database conn
  end

  def test_cleanse_statement
    fake_redshift_copy_stmt = %{copy schema_name.table_name
      from 's3://customerbucket/some_file_path'
      with credentials 'aws_access_key_id=AKKKKKBBBBBLMA3HAAAA;aws_secret_access_key=QQQQQEQeeeeeenUWiCgWWWuzRjwZZZZZtOvU9alP'
      gzip     
      removequotes 
      escape;}

    cleansed_redshift_copy_stmt = "copy schema_name.table_name from 's3://customerbucket/some_file_path' with credentials '<removed><removed>' gzip removequotes escape;"
      
    b = JDBCHelpers::Base.new
    assert_equal(b.cleanse_statement(fake_redshift_copy_stmt), cleansed_redshift_copy_stmt)
  end
  
  def test_query_results_to_array
    conn = give_me_a_simple_test_database
    
    a=JDBCHelpers::QueryResultsToArray.new(conn, "select * from test;").results
    
    assert a.class == Array
    assert a.length == 2
    assert a[0].keys.sort == ['a', 'b', 'c', 'd']
    assert a[1].keys.sort == ['a', 'b', 'c', 'd']
    
    a.each { |h| assert h['d'] == '2016-07-01' }
    
    #clean up
    close_simple_test_database conn
  end
  
  def test_query_results_to_hash_of_arrays
    conn = give_me_a_simple_test_database
    
    a=JDBCHelpers::QueryResultsToHashOfArrays.new(conn, "select * from test;",'d').results
    
    assert a.class == Hash
    assert a.keys == ['2016-07-01']
    assert a['2016-07-01'].class == Array
    assert a['2016-07-01'].length == 2
    
    assert a['2016-07-01'][0].keys.sort == ['a', 'b', 'c', 'd']
    assert a['2016-07-01'][1].keys.sort == ['a', 'b', 'c', 'd']

    b=JDBCHelpers::QueryResultsToHashOfArrays.new(conn,"select * from test;",'a').results
    
    assert a.class == Hash
    
    assert b.keys == ['12345', '12346']
    assert b['12345'].length == 1
    assert b['12346'].length == 1
    
    assert b['12345'][0].keys.sort == ['a', 'b', 'c', 'd']
    assert b['12346'][0].keys.sort == ['a', 'b', 'c', 'd']
    
    #clean up
    close_simple_test_database conn
  end  
  
  def test_query_results_to_json_file
    conn = give_me_a_simple_test_database
    
    #first test a basic json export
    json_file_path="#{File.dirname(__FILE__)}/tmp/test_json.json"
    File.delete(json_file_path) if File.exists?(json_file_path)
    f=File.open(json_file_path, 'w')
    dump=JDBCHelpers::QueryResultsToJSONFile.new(conn, 'select a,b,c,d from test;',f)
    f.close
    assert_equal(
      File.read(json_file_path),
      File.read("#{File.dirname(__FILE__)}/fixtures/json_export.json")
    )
    
    #now let's get fancy with a custom formatter
    pipe_delim_file_path = "#{File.dirname(__FILE__)}/tmp/test_pipe_delim.txt"
    File.delete(pipe_delim_file_path) if File.exists?(pipe_delim_file_path)
    f = File.open(pipe_delim_file_path,'w')
    dump=JDBCHelpers::QueryResultsToJSONFile.new(
      conn,
      'select a,b,c,d from test;',
      f,
      Proc.new {|f,h| f.puts h.keys.map{|k| h[k]}.join('|')}
    )
    f.close
    assert_equal(
      File.read(pipe_delim_file_path),
      File.read("#{File.dirname(__FILE__)}/fixtures/pipe_delim_export.txt") 
    )
    
    #clean up
    File.delete(json_file_path) if File.exists?(json_file_path)
    File.delete(pipe_delim_file_path) if File.exists?(pipe_delim_file_path)
    close_simple_test_database(conn)
  end
end