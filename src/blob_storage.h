#pragma once
#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif
#include <inttypes.h>
#include "blob_file_cache.h"
#include "blob_format.h"
#include "blob_gc.h"
#include "rocksdb/options.h"
#include "titan_stats.h"

namespace rocksdb {
namespace titandb {

// Provides methods to access the blob storage for a specific
// column family. The version must be valid when this storage is used.
class BlobStorage {
 public:
  BlobStorage(const BlobStorage& bs) : destroyed_(false) {
    this->files_ = bs.files_;
    this->file_cache_ = bs.file_cache_;
    this->db_options_ = bs.db_options_;
    this->cf_options_ = bs.cf_options_;
    this->cf_id_ = bs.cf_id_;
    this->stats_ = bs.stats_;
  }

  BlobStorage(const TitanDBOptions& _db_options,
              const TitanCFOptions& _cf_options, uint32_t cf_id,
              std::shared_ptr<BlobFileCache> _file_cache, TitanStats* stats)
      : db_options_(_db_options),
        cf_options_(_cf_options),
        cf_id_(cf_id),
        file_cache_(_file_cache),
        destroyed_(false),
        stats_(stats) {}

  ~BlobStorage() {
    for (auto& file : files_) {
      file_cache_->Evict(file.second->file_number());
    }
  }

  // Gets the blob record pointed by the blob index. The provided
  // buffer is used to store the record data, so the buffer must be
  // valid when the record is used.
  Status Get(const ReadOptions& options, const BlobIndex& index,
             BlobRecord* record, PinnableSlice* buffer);

  // Creates a prefetcher for the specified file number.
  Status NewPrefetcher(uint64_t file_number,
                       std::unique_ptr<BlobFilePrefetcher>* result);

  // Finds the blob file meta for the specified file number. It is a
  // corruption if the file doesn't exist.
  std::weak_ptr<BlobFileMeta> FindFile(uint64_t file_number) const;

  std::size_t NumBlobFiles() const {
    MutexLock l(&mutex_);
    return files_.size();
  }

  void ExportBlobFiles(
      std::map<uint64_t, std::weak_ptr<BlobFileMeta>>& ret) const;

  void MarkAllFilesForGC() {
    MutexLock l(&mutex_);
    for (auto& file : files_) {
      file.second->set_gc_mark(true);
      file.second->FileStateTransit(BlobFileMeta::FileEvent::kDbRestart);
    }
  }

  // For Test only
  void TEST_MarkAllFilesForGC() const {
    MutexLock l(&mutex_);
    for (auto& file : files_) {
      file.second->set_gc_mark(true);
    }
  }

  // For Test only
  std::unordered_map<uint64_t, BlobFileMeta> TEST_GetAllFiles() const {
    std::unordered_map<uint64_t, BlobFileMeta> _files;
    for (const auto& iter : files_) {
      _files.insert({iter.first, *iter.second});
    }
    return _files;
  }

  void MarkDestroyed() {
    MutexLock l(&mutex_);
    destroyed_ = true;
  }

  bool MaybeRemove() const {
    MutexLock l(&mutex_);
    return destroyed_ && obsolete_files_.empty();
  }

  const std::vector<GCScore> gc_score() {
    MutexLock l(&mutex_);
    return gc_score_;
  }

  void ComputeGCScore();

  const TitanDBOptions& db_options() { return db_options_; }

  const TitanCFOptions& cf_options() { return cf_options_; }

  void AddBlobFile(std::shared_ptr<BlobFileMeta>& file);

  void GetObsoleteFiles(std::vector<std::string>* obsolete_files,
                        SequenceNumber oldest_sequence);

  void MarkFileObsolete(std::shared_ptr<BlobFileMeta> file,
                        SequenceNumber obsolete_sequence);

 private:
  friend class VersionSet;
  friend class VersionTest;
  friend class BlobGCPickerTest;
  friend class BlobGCJobTest;
  friend class BlobFileSizeCollectorTest;

  TitanDBOptions db_options_;
  TitanCFOptions cf_options_;
  uint32_t cf_id_;

  mutable port::Mutex mutex_;

  // Only BlobStorage OWNS BlobFileMeta
  std::unordered_map<uint64_t, std::shared_ptr<BlobFileMeta>> files_;
  std::shared_ptr<BlobFileCache> file_cache_;

  std::vector<GCScore> gc_score_;

  std::list<std::pair<uint64_t, SequenceNumber>> obsolete_files_;
  // It is marked when the column family handle is destroyed, indicating the
  // in-memory data structure can be destroyed. Physical files may still be
  // kept.
  bool destroyed_;

  TitanStats* stats_;
};

}  // namespace titandb
}  // namespace rocksdb
