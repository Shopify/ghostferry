require 'rake/testtask'

task :default => [:test]

Rake::TestTask.new do |t|
  t.test_files = FileList['test/**/*test.rb']
  t.verbose = true
  t.libs << ["test", "test/helpers", "test/lib"]
end
