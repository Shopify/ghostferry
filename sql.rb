queries = []
10.times do
  shop_id = rand(1..5)

  if shop_id > 3
    product_types = 200.times.map { |i| "product_type_#{i}" }
  else
    product_types = 10.times.map { |i| "product_type_#{i}" }
  end

  queries <<
    "insert into products (tenant_id, handle, product_type, is_not_deleted, is_published, created_at) values (#{shop_id}, 'foobar', '#{product_types.sample}', 1, 1, NOW());"

end
puts queries.join("\n")
