#!/usr/bin/env ruby

require "json"
require "logger"
require "webrick"

STDOUT.sync = true

logger = Logger.new(STDERR)
logger.level = Logger::INFO

server = WEBrick::HTTPServer.new(
  BindAddress: "127.0.0.1",
  Port: 8002,
)

# Needed for the main container to tell that the sidecar containers are up.
server.mount_proc "/ping" do |req, resp|
  resp.body = "PONG"
end

server.mount_proc "/callbacks/progress" do |req, resp|
  unless req.body
    logger.warn("ghostferry did not send any data while calling progress")
    next
  end

  progress_data = JSON.parse(JSON.parse(req.body)["Payload"])
  puts JSON.generate(progress_data)
  resp.body = "OK"
end

server.mount_proc "/callbacks/state" do |req, resp|
  unless req.body
    logger.warn("ghostferry did not send any data while calling progress")
    next
  end

  progress_data = JSON.parse(JSON.parse(req.body)["Payload"])
  logger.info("received state: #{progress_data}")
  resp.body = "OK"
end

server.start
