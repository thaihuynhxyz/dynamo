#pragma once

#include <filesystem>

#include "pagebroker.pb.h"

namespace snapshot::pagebroker {

class Broker {
 public:
  Broker(std::filesystem::path working_directory, std::filesystem::path checkpoint_storage_directory);

  Response Handle(const Request& request);

 private:
  Response Restore(const RestoreRequest& request);
  Response Prepare(const PrepareCheckpointRequest& request);
  Response Commit(const CommitRequest& request);
  Response Abort(const AbortRequest& request);
  std::filesystem::path TemporaryCheckpointsDirectory() const;
  std::filesystem::path StagedRestoresDirectory() const;

  std::filesystem::path working_directory_;
  std::filesystem::path checkpoint_storage_directory_;
};

}  // namespace snapshot::pagebroker
