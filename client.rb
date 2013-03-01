require 'ffi-rzmq'

context = ZMQ::Context.new
socket = context.socket(ZMQ::REQ)
socket.connect('ipc://frontend')

10.times do |request|
  string = "Hello #{request}"
  socket.send_string(string)
  puts "Sending #{string}"
  socket.recv_string(message = '')
  puts "Received reply #{request}\t#{message}"
end
