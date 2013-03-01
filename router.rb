require 'ffi-rzmq'

context = ZMQ::Context.new
frontend = context.socket(ZMQ::ROUTER)
backend = context.socket(ZMQ::DEALER)

frontend.bind('ipc://frontend')
backend.bind('ipc://backend')

Signal.trap("INT") do
  $stdout.write "Shutting down...\n"
  frontend.close
  backend.close
  context.terminate
  exit
end

poller = ZMQ::Device.new(ZMQ::QUEUE, frontend, backend)

loop do
  poller.poll(:blocking)
  poller.readables.each do |socket|
    if socket === frontend
      loop do
        socket.recv_string(message = '')
        more = socket.more_parts?
        backend.send_string(message, more ? ZMQ::SNDMORE : 0)
        break unless more
      end
    elsif socket === backend
      loop do
        socket.recv_string(message = '')
        more = socket.more_parts?
        frontend.send_string(message, more ? ZMQ::SNDMORE : 0)
        break unless more
      end
    end
  end
end
