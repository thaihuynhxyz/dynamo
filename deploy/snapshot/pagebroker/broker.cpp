#include "broker.hpp"

#include <sys/statvfs.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <random>
#include <stdexcept>

namespace snapshot::pagebroker {
namespace {

namespace fs = std::filesystem;

Response
Reply(bool ok, std::string transaction_id = {}, std::string directory_path = {}, std::string error = {})
{
  Response response;
  response.set_ok(ok);
  if (!transaction_id.empty())
    response.set_transaction_id(transaction_id);
  if (!directory_path.empty())
    response.set_directory_path(directory_path);
  if (!error.empty())
    response.set_error(error);
  return response;
}

bool
Within(const fs::path& root, const fs::path& path)
{
  auto canonical_root = fs::weakly_canonical(root);
  auto canonical_path = fs::weakly_canonical(path);
  auto mismatch =
      std::mismatch(canonical_root.begin(), canonical_root.end(), canonical_path.begin(), canonical_path.end());
  return mismatch.first == canonical_root.end();
}

uintmax_t
TreeSize(const fs::path& path)
{
  uintmax_t size = 0;
  for (const auto& entry : fs::recursive_directory_iterator(path)) {
    if (entry.is_symlink())
      throw std::runtime_error("checkpoint contains symlink");
    if (entry.is_regular_file())
      size += entry.file_size();
  }
  return size;
}

bool
HasSpace(const fs::path& root, uintmax_t bytes)
{
  struct statvfs stat {};
  return statvfs(root.c_str(), &stat) == 0 && uintmax_t(stat.f_bavail) * stat.f_frsize >= bytes;
}

std::string
ID()
{
  static std::mt19937_64 random(std::random_device{}());
  return std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + "-" + std::to_string(random());
}

}  // namespace

Broker::Broker(fs::path working_directory, fs::path checkpoint_storage_directory)
    : working_directory_(fs::weakly_canonical(std::move(working_directory))),
      checkpoint_storage_directory_(fs::weakly_canonical(std::move(checkpoint_storage_directory))),
      temporary_checkpoints_directory_(working_directory_ / "transactions"),
      staged_restore_directory_(working_directory_ / "staged")
{
  fs::create_directories(temporary_checkpoints_directory_);
  fs::create_directories(staged_restore_directory_);
}

Response
Broker::Handle(const Request& request)
{
  switch (request.command_case()) {
    case Request::kRestore: {
      const auto& restore = request.restore();
      if (!restore.has_source_path() || !restore.has_mode() || restore.mode() == RestoreRequest::MODE_UNSPECIFIED)
        return Reply(false, {}, {}, "invalid restore request");
      return Restore(restore);
    }
    case Request::kPrepareCheckpoint: {
      const auto& prepare = request.prepare_checkpoint();
      if (!prepare.has_destination_path() || prepare.destination_path().empty())
        return Reply(false, {}, {}, "invalid checkpoint request");
      return Prepare(prepare);
    }
    case Request::kCommit: {
      const auto& commit = request.commit();
      if (!commit.has_transaction_id() || commit.transaction_id().empty())
        return Reply(false, {}, {}, "invalid checkpoint transaction");
      return Commit(commit);
    }
    case Request::kAbort: {
      const auto& abort = request.abort();
      if (!abort.has_transaction_id() || abort.transaction_id().empty())
        return Reply(false, {}, {}, "invalid transaction");
      return Abort(abort);
    }
    default:
      return Reply(false, {}, {}, "unknown operation");
  }
}

Response
Broker::Restore(const RestoreRequest& request)
{
  fs::path source(request.source_path());
  if (!source.is_absolute() || !Within(checkpoint_storage_directory_, source) || !fs::is_directory(source))
    return Reply(false, {}, {}, "checkpoint path must be an existing directory in checkpoint storage");
  if (request.mode() == RestoreRequest::MODE_DIRECT)
    return Reply(true, {}, source.string());
  if (request.mode() != RestoreRequest::MODE_STAGED)
    return Reply(false, {}, {}, "invalid restore mode");
  uintmax_t bytes;
  try {
    bytes = TreeSize(source);
  }
  catch (const std::exception& e) {
    return Reply(false, {}, {}, e.what());
  }
  if (!HasSpace(working_directory_, bytes))
    return Reply(false, {}, {}, "insufficient staging disk budget");
  auto destination = staged_restore_directory_ / ID();
  auto temporary = staged_restore_directory_ / ("." + destination.filename().string());
  try {
    fs::copy(source, temporary, fs::copy_options::recursive);
    fs::rename(temporary, destination);
  }
  catch (const std::exception& e) {
    fs::remove_all(temporary);
    return Reply(false, {}, {}, e.what());
  }
  return Reply(true, destination.filename().string(), destination.string());
}

Response
Broker::Prepare(const PrepareCheckpointRequest& request)
{
  fs::path final(request.destination_path());
  if (!final.is_absolute() || !Within(checkpoint_storage_directory_, final))
    return Reply(false, {}, {}, "checkpoint path must be in checkpoint storage");
  auto id = ID();
  fs::path path = temporary_checkpoints_directory_ / id;
  fs::path target = temporary_checkpoints_directory_ / (id + ".target");
  try {
    fs::create_directory(path);
    std::ofstream output(target);
    output << final.string() << '\n';
    if (!output)
      throw std::runtime_error("create transaction target");
  }
  catch (const std::exception& e) {
    fs::remove_all(path);
    fs::remove(target);
    return Reply(false, {}, {}, e.what());
  }
  return Reply(true, id, path.string());
}

Response
Broker::Commit(const CommitRequest& request)
{
  fs::path transaction = temporary_checkpoints_directory_ / request.transaction_id();
  fs::path target = temporary_checkpoints_directory_ / (request.transaction_id() + ".target");
  if (request.transaction_id().empty() || !Within(temporary_checkpoints_directory_, transaction) ||
      !Within(temporary_checkpoints_directory_, target) || !fs::is_directory(transaction) ||
      !fs::is_regular_file(target))
    return Reply(false, {}, {}, "invalid checkpoint transaction");
  std::ifstream input(target);
  std::string destination;
  std::getline(input, destination);
  fs::path final(destination);
  if (!input || !final.is_absolute() || !Within(checkpoint_storage_directory_, final))
    return Reply(false, {}, {}, "invalid checkpoint transaction");
  auto promotion = final.parent_path() / ("." + final.filename().string() + ".pagebroker-" + request.transaction_id());
  try {
    if (fs::exists(final) || fs::exists(promotion))
      return Reply(false, {}, {}, "checkpoint destination already exists");
    fs::create_directories(final.parent_path());
    fs::copy(transaction, promotion, fs::copy_options::recursive);
    fs::rename(promotion, final);
    fs::remove_all(transaction);
    fs::remove(target);
  }
  catch (const std::exception& e) {
    fs::remove_all(promotion);
    return Reply(false, {}, {}, e.what());
  }
  return Reply(true, request.transaction_id(), final.string());
}

Response
Broker::Abort(const AbortRequest& request)
{
  fs::path transaction = temporary_checkpoints_directory_ / request.transaction_id();
  fs::path staged = staged_restore_directory_ / request.transaction_id();
  fs::path target = temporary_checkpoints_directory_ / (request.transaction_id() + ".target");
  if (request.transaction_id().empty() || !Within(temporary_checkpoints_directory_, transaction) ||
      !Within(temporary_checkpoints_directory_, target) || !Within(staged_restore_directory_, staged))
    return Reply(false, {}, {}, "invalid transaction");
  try {
    fs::remove_all(transaction);
    fs::remove(target);
    fs::remove_all(staged);
  }
  catch (const std::exception& e) {
    return Reply(false, {}, {}, e.what());
  }
  return Reply(true, request.transaction_id());
}

}  // namespace snapshot::pagebroker
