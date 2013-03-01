require 'ffi-rzmq'

context = ZMQ::Context.new
socket = context.socket(ZMQ::REP)
socket.connect('ipc://backend')

Signal.trap("INT") do
  $stdout.write "Shutting down...\n"
  socket.close
  context.terminate
  exit
end

loop do
  socket.recv_string(message = '')
  puts "Received #{message}"
  socket.send_string("World")
end
