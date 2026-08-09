#include <sys/statvfs.h>

#include <cassert>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <random>

#include "broker.hpp"

using namespace snapshot::pagebroker;

namespace fs = std::filesystem;

std::string
TestID()
{
  static std::mt19937_64 random(std::random_device{}());
  return std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + "-" + std::to_string(random());
}

Request
MakeRestore(std::string source_path, RestoreRequest::Mode mode = RestoreRequest::MODE_DIRECT)
{
  Request request;
  request.mutable_restore()->set_source_path(source_path);
  request.mutable_restore()->set_mode(mode);
  return request;
}
Request
MakePrepare(std::string destination_path)
{
  Request request;
  request.mutable_prepare_checkpoint()->set_destination_path(destination_path);
  return request;
}
Request
MakeCommit(std::string transaction_id)
{
  Request request;
  request.mutable_commit()->set_transaction_id(transaction_id);
  return request;
}
Request
MakeAbort(std::string transaction_id)
{
  Request request;
  request.mutable_abort()->set_transaction_id(transaction_id);
  return request;
}

int
main()
{
  auto root = fs::temp_directory_path() / ("pagebroker-test-" + TestID());
  auto checkpoints = root / "checkpoints";
  auto source = checkpoints / "source";
  fs::create_directories(source);
  std::ofstream(source / "manifest.json") << "checkpoint";
  Broker broker(root / "nvme", checkpoints);
  assert(!broker.Handle(Request{}).ok());
  Request missing_mode;
  missing_mode.mutable_restore()->set_source_path(source.string());
  assert(!broker.Handle(missing_mode).ok());
  Request unspecified_mode;
  unspecified_mode.mutable_restore()->set_source_path(source.string());
  unspecified_mode.mutable_restore()->set_mode(RestoreRequest::MODE_UNSPECIFIED);
  assert(!broker.Handle(unspecified_mode).ok());
  Request missing_destination;
  missing_destination.mutable_prepare_checkpoint();
  assert(!broker.Handle(missing_destination).ok());
  Request missing_transaction;
  missing_transaction.mutable_commit();
  assert(!broker.Handle(missing_transaction).ok());
  auto direct = broker.Handle(MakeRestore(source.string()));
  assert(direct.ok() && direct.directory_path() == source.string());
  auto staged = broker.Handle(MakeRestore(source.string(), RestoreRequest::MODE_STAGED));
  assert(staged.ok() && fs::exists(fs::path(staged.directory_path()) / "manifest.json"));
  assert(broker.Handle(MakeAbort(staged.transaction_id())).ok());
  assert(!fs::exists(staged.directory_path()));
  assert(!broker.Handle(MakeRestore("relative")).ok());
  auto outside = root / "outside";
  fs::create_directories(outside);
  assert(!broker.Handle(MakeRestore(outside.string())).ok());
  struct statvfs disk {};
  assert(statvfs((root / "nvme").c_str(), &disk) == 0);
  std::ofstream oversized(source / "oversized.img");
  oversized.seekp(static_cast<std::streamoff>(uintmax_t(disk.f_bavail) * disk.f_frsize));
  oversized.put('\0');
  oversized.close();
  assert(!broker.Handle(MakeRestore(source.string(), RestoreRequest::MODE_STAGED)).ok());
  fs::remove(source / "oversized.img");
  Request malformed;
  assert(!malformed.ParseFromString("\x08\x01\x1a\x80"));
  auto final = checkpoints / "one";
  auto prepared = broker.Handle(MakePrepare(final.string()));
  assert(prepared.ok());
  std::ofstream(fs::path(prepared.directory_path()) / "image.img") << "image";
  auto committed = broker.Handle(MakeCommit(prepared.transaction_id()));
  assert(committed.ok() && fs::exists(final / "image.img"));
  assert(!broker.Handle(MakePrepare(outside.string())).ok());
  auto aborted = broker.Handle(MakePrepare((checkpoints / "two").string()));
  assert(aborted.ok() && broker.Handle(MakeAbort(aborted.transaction_id())).ok());
  assert(!broker.Handle(MakeCommit(aborted.transaction_id())).ok());
  fs::remove_all(root);
}
