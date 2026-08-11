#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <array>
#include <cstring>
#include <iostream>
#include <stdexcept>

#include "broker.hpp"

namespace {

using snapshot::pagebroker::Broker;
using snapshot::pagebroker::Request;
using snapshot::pagebroker::Response;

std::string
Encode(const Response& response)
{
  std::string encoded;
  response.SerializeToString(&encoded);
  return encoded;
}

std::string
Error(const std::string& message)
{
  Response response;
  response.set_error(message);
  return Encode(response);
}

void
Serve(int client, Broker* broker)
{
  std::array<char, 65536> bytes{};
  iovec iov{bytes.data(), bytes.size()};
  msghdr message{};
  message.msg_iov = &iov;
  message.msg_iovlen = 1;
  auto received = recvmsg(client, &message, 0);
  if (received <= 0)
    return;
  if (message.msg_flags & MSG_TRUNC) {
    auto response = Error("PageBroker request exceeds 64 KiB");
    send(client, response.data(), response.size(), MSG_NOSIGNAL);
    return;
  }
  Request request;
  std::string response = request.ParseFromArray(bytes.data(), received) ? Encode(broker->HandleRequest(request))
                                                                        : Error("invalid PageBroker request");
  send(client, response.data(), response.size(), MSG_NOSIGNAL);
}

}  // namespace

int
main(int argc, char** argv)
{
  if (argc != 4) {
    std::cerr << "usage: pagebroker-daemon SOCKET NVME_ROOT CHECKPOINT_ROOT\n";
    return 2;
  }
  try {
    Broker broker(argv[2], argv[3]);
    unlink(argv[1]);
    int server = socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0);
    if (server < 0)
      throw std::runtime_error("socket failed");
    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    if (std::string(argv[1]).size() >= sizeof(address.sun_path))
      throw std::runtime_error("socket path too long");
    std::strcpy(address.sun_path, argv[1]);
    if (bind(server, reinterpret_cast<sockaddr*>(&address), sizeof(address)) || listen(server, 16))
      throw std::runtime_error("bind failed");
    for (;;) {
      int client = accept4(server, nullptr, nullptr, SOCK_CLOEXEC);
      if (client >= 0) {
        Serve(client, &broker);
        close(client);
      }
    }
  }
  catch (const std::exception& e) {
    std::cerr << e.what() << '\n';
    return 1;
  }
}
