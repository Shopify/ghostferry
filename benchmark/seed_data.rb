#!/usr/bin/env ruby

require "optparse"
require_relative "./ghostferry_benchmark"

options = {
  nrows: 100_000,
  ncols: 2,
  row_size: 10000,
  insert_batch_size: 300,
}

opts = OptionParser.new do |opts|
  opts.banner = "Usage: seed_data.rb [options]"

  opts.on("-r", "--nrows ROWS", "Number of rows") do |v|
    options[:nrows] = v.to_i
  end

  opts.on("-c", "--ncols COLS", "Number of columns") do |v|
    options[:ncols] = v.to_i
  end

  opts.on("-s", "--row-size SIZE", "Approximate row size in bytes.") do |v|
    options[:row_size] = v.to_i
  end

  opts.on("-b", "--insert-batch-size BATCH_SIZE", "Batch size of the insert during the seed operation") do |v|
    options[:insert_batch_size] = v.to_i
  end
end

opts.parse!

puts options

GhostferryBenchmark::Databases.seed(
  nrows: options[:nrows],
  ncols: options[:ncols],
  row_size: options[:row_size],
  insert_batch_size: options[:insert_batch_size],
)
