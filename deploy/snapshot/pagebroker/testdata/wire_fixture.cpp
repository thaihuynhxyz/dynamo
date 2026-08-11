#include <iostream>

#include "pagebroker.pb.h"
using namespace snapshot::pagebroker;
int
main(int argc, char** argv)
{
  if (argc == 1) {
    Response response;
    auto* restore = response.mutable_restore();
    restore->set_transaction_id("txn");
    restore->set_directory_path("/checkpoint");
    std::string encoded;
    if (!response.SerializeToString(&encoded))
      return 1;
    std::cout << encoded;
    return 0;
  }
  if (argc != 2 || std::string(argv[1]) != "decode")
    return 2;
  Request request;
  if (!request.ParseFromIstream(&std::cin))
    return 1;
  if (!request.has_restore())
    return 1;
  const auto& restore = request.restore();
  std::cout << restore.source_path() << ' ' << RestoreRequest::Mode_Name(restore.mode()) << '\n';
}
