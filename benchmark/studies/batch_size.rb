#!/usr/bin/env ruby

require "fileutils"

require_relative "../ghostferry_benchmark.rb"

row_sizes = [1, 100, 200, 500]
batch_sizes = (5000..10000).step(1000).to_a

max_total_bytes = 100_000_000

puts "Computing benchmarks on #{row_sizes.length} row sizes and #{batch_sizes.length} batch_sizes for a total of #{row_sizes.length * batch_sizes.length} cases"

File.open("studies/batch_size_benchmark.csv", "w") do |f|
  row_sizes.tqdm.each do |row_size|
    GhostferryBenchmark::Databases.seed(
      nrows: (max_total_bytes / row_size).clamp(50000, 2_000_000),
      ncols: 2,
      row_size: row_size,
      insert_batch_size: (60000 / row_size).round.to_i.clamp(200, 12000)
    )

    batch_sizes.each do |batch_size|
      output_dir = "out/rs=#{row_size}-bs=#{batch_size}"
      FileUtils.mkdir_p(output_dir)

      config = GhostferryBenchmark.default_ghostferry_config
      config["DataIterationBatchSize"] = batch_size

      GhostferryBenchmark::Databases.wipe_target
      speed = GhostferryBenchmark.run_ghostferry(ghostferry_config: config, output_dir: output_dir)
      f.puts("#{row_size},#{batch_size},#{speed}")
    end
  end
end
