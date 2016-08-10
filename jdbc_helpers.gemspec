Gem::Specification.new do |s|
  s.name        = 'jdbc_helpers'
  s.version     = '0.0.3'
  s.date        = '2016-07-01'
  s.summary     = "helpers for jdbc interaction"
  s.description = "allows for easy conversion of query results to hashes, arrays of hashes, json files, etc."
  s.authors     = ["jeremy winters"]
  s.email       = 'jeremy.winters@full360.com'
  s.files       = ["lib/jdbc_helpers.rb"]
  s.homepage    = 'https://www.full360.com'
  s.license       = 'MIT'
  s.add_runtime_dependency 'logger','>=1.2.8'
  s.add_development_dependency 'minitest','>=5.9.0'
  s.platform = 'java'
end