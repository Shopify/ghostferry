require 'bundler/setup'

require 'lhm'
require 'active_record'

ActiveRecord::Base.establish_connection(
  adapter: 'mysql2',
  host: '127.0.0.1',
  database: 'gftest1',
  port: '29291',
  username: 'root'
)

Lhm.change_table :products do |m|
  m.add_column :locale, "VARCHAR(2)"
end