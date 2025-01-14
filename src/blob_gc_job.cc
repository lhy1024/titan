#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif
#include <inttypes.h>

#include "blob_gc_job.h"
#include "dig_hole_job.h"
#include "env/io_posix.h"
namespace rocksdb {
namespace titandb {

// Write callback for garbage collection to check if key has been updated
// since last read. Similar to how OptimisticTransaction works.
class BlobGCJob::GarbageCollectionWriteCallback : public WriteCallback {
 public:
  GarbageCollectionWriteCallback(ColumnFamilyHandle* cfh, std::string&& _key,
                                 BlobIndex&& blob_index)
      : cfh_(cfh), key_(std::move(_key)), blob_index_(blob_index) {
    assert(!key_.empty());
  }

  std::string value;

  virtual Status Callback(DB* db) override {
    auto* db_impl = reinterpret_cast<DBImpl*>(db);
    PinnableSlice index_entry;
    bool is_blob_index;
    auto s = db_impl->GetImpl(ReadOptions(), cfh_, key_, &index_entry,
                              nullptr /*value_found*/,
                              nullptr /*read_callback*/, &is_blob_index);
    if (!s.ok() && !s.IsNotFound()) {
      return s;
    }
    read_bytes_ = key_.size() + index_entry.size();
    if (s.IsNotFound()) {
      // Either the key is deleted or updated with a newer version which is
      // inlined in LSM.
      s = Status::Busy("key deleted");
    } else if (!is_blob_index) {
      s = Status::Busy("key overwritten with other value");
    }

    if (s.ok()) {
      BlobIndex other_blob_index;
      s = other_blob_index.DecodeFrom(&index_entry);
      if (!s.ok()) {
        return s;
      }

      if (!(blob_index_ == other_blob_index)) {
        s = Status::Busy("key overwritten with other blob");
      }
    }

    return s;
  }

  virtual bool AllowWriteBatching() override { return false; }

  std::string key() { return key_; }

  uint64_t read_bytes() { return read_bytes_; }

  uint64_t blob_record_size() { return blob_index_.blob_handle.size; }

 private:
  ColumnFamilyHandle* cfh_;
  // Key to check
  std::string key_;
  BlobIndex blob_index_;
  uint64_t read_bytes_;
};

BlobGCJob::BlobGCJob(BlobGC* blob_gc, DB* db, port::Mutex* mutex,
                     const TitanDBOptions& titan_db_options, Env* env,
                     const EnvOptions& env_options,
                     BlobFileManager* blob_file_manager,
                     VersionSet* version_set, LogBuffer* log_buffer,
                     std::atomic_bool* shuting_down, TitanStats* stats)
    : blob_gc_(blob_gc),
      base_db_(db),
      base_db_impl_(reinterpret_cast<DBImpl*>(base_db_)),
      mutex_(mutex),
      db_options_(titan_db_options),
      env_(env),
      env_options_(env_options),
      blob_file_manager_(blob_file_manager),
      version_set_(version_set),
      log_buffer_(log_buffer),
      shuting_down_(shuting_down),
      stats_(stats) {
  dig_hole_job_ = std::make_shared<DigHoleJob>(
      titan_db_options, env_options, env, blob_gc->titan_cf_options(),
      std::bind(&BlobGCJob::IsShutingDown, this),
      std::bind(&BlobGCJob::DiscardEntry, this, std::placeholders::_1,
                std::placeholders::_2, std::placeholders::_3));
}

BlobGCJob::~BlobGCJob() {
  if (log_buffer_) {
    log_buffer_->FlushBufferToLog();
    LogFlush(db_options_.info_log.get());
  }
  // flush metrics
  RecordTick(stats_, BLOB_DB_BYTES_READ, metrics_.blob_db_bytes_read);
  RecordTick(stats_, BLOB_DB_BYTES_WRITTEN, metrics_.blob_db_bytes_written);
  RecordTick(stats_, BLOB_DB_GC_NUM_KEYS_OVERWRITTEN,
             metrics_.blob_db_gc_num_keys_overwritten);
  RecordTick(stats_, BLOB_DB_GC_BYTES_OVERWRITTEN,
             metrics_.blob_db_gc_bytes_overwritten);
  RecordTick(stats_, BLOB_DB_GC_NUM_KEYS_RELOCATED,
             metrics_.blob_db_gc_num_keys_relocated);
  RecordTick(stats_, BLOB_DB_GC_BYTES_RELOCATED,
             metrics_.blob_db_gc_bytes_relocated);
  RecordTick(stats_, BLOB_DB_GC_NUM_NEW_FILES,
             metrics_.blob_db_gc_num_new_files);
  RecordTick(stats_, BLOB_DB_GC_NUM_FILES, metrics_.blob_db_gc_num_files);
}

Status BlobGCJob::Prepare() { return Status::OK(); }

Status BlobGCJob::Run() {
  Status s = SampleCandidateFiles();
  if (!s.ok()) {
    return s;
  }

  if (!blob_gc_->gc_sample_inputs().empty()) {
    s = DoRunGC();
    if (!s.ok()) {
      return s;
    }
  }

  if (!blob_gc_->fs_sample_inputs().empty()) {
    s = DigHole();
    if (!s.ok()) {
      return s;
    }
  }

  return Status::OK();
}

Status BlobGCJob::SampleCandidateFiles() {
  std::vector<BlobFileMeta*> gc_sample_inputs;
  std::vector<BlobFileMeta*> fs_sample_inputs;
  std::unordered_set<uint64_t> gc_selected_marks;

  // select files for GC
  for (const auto& file : blob_gc_->gc_inputs()) {
    bool selected = false;
    if (file->GetValidSize() <=
        blob_gc_->titan_cf_options().merge_small_file_threshold) {
      selected = true;
    } else {
      // smaple files with gc marked see if we can GC it
      // we can not do free space on these files because we can not get
      // the correct discardable_size on these files
      Status s = DoSample(file, &selected);
      if (!s.ok()) {
        return s;
      }
    }
    if (selected) {
      gc_selected_marks.insert(file->file_number());
      gc_sample_inputs.push_back(file);

      ROCKS_LOG_INFO(db_options_.info_log,
                     "Titan selected GC file %" PRIu64 ".",
                     file->file_number());
    }
  }

  // select files for free space
  for (const auto& file : blob_gc_->fs_inputs()) {
    // don't select files that already be selected to GC or has gc marked
    if (gc_selected_marks.find(file->file_number()) !=
            gc_selected_marks.end() ||
        file->gc_mark()) {
      continue;
    }
    bool selected = false;
    if (file->discardable_size() >=
        static_cast<int64_t>(
            blob_gc_->titan_cf_options().free_space_threshold)) {
      selected = true;
    } else if (blob_gc_->titan_cf_options().fast_reclaim_space_by_sample) {
      // because discardable_size are lazily captured by compaction on LSM tree
      // and can not reflect the real-time discardable_size, by sample files we
      // can query LSM tree with the most up to date discardable information and
      // hence more quickly reclaim disk space
      Status s = DoSample(file, &selected);
      if (!s.ok()) {
        return s;
      }
    }
    if (selected) {
      fs_sample_inputs.push_back(file);

      ROCKS_LOG_INFO(db_options_.info_log,
                     "Titan selected dig hole file %" PRIu64 ".",
                     file->file_number());
    }
  }

  if (!gc_sample_inputs.empty()) {
    blob_gc_->set_gc_sample_inputs(std::move(gc_sample_inputs));
  }
  if (!fs_sample_inputs.empty()) {
    blob_gc_->set_fs_sample_inputs(std::move(fs_sample_inputs));
  }
  return Status::OK();
}

Status BlobGCJob::DoSample(const BlobFileMeta* file, bool* selected) {
  assert(selected != nullptr);
  Status s;

  std::unique_ptr<PosixRandomRWFile> file_reader;
  s = OpenBlobFile(file->file_number(), 0, db_options_, env_options_, env_,
                   &file_reader);
  if (!s.ok()) {
    return s;
  }
  BlobFileIterator iter(std::move(file_reader), file->file_number(),
                        file->file_size(), blob_gc_->titan_cf_options());
  iter.SeekToFirst();
  if (!iter.status().ok()) {
    s = iter.status();
    ROCKS_LOG_ERROR(db_options_.info_log,
                    "SeekToFirst failed, file number[%" PRIu64 "] size[%" PRIu64
                    "] status[%s]",
                    file->file_number(), file->file_size(),
                    s.ToString().c_str());
    return s;
  }
  assert(iter.Valid());

  uint64_t iterated_size{0};
  uint64_t discardable_size{0};
  for (; iter.Valid(); iter.Next()) {
    BlobIndex blob_index = iter.GetBlobIndex();
    uint64_t total_length = blob_index.blob_handle.size;
    iterated_size += total_length;
    bool discardable = false;
    s = DiscardEntry(iter.key(), blob_index, &discardable);
    if (!s.ok()) {
      return s;
    }
    if (discardable) {
      discardable_size += total_length;
    }
  }
  metrics_.blob_db_bytes_read += iterated_size;
  assert(iter.status().ok());

  *selected =
      discardable_size >=
      std::ceil(iterated_size *
                blob_gc_->titan_cf_options().blob_file_discardable_ratio);
  return s;
}

Status BlobGCJob::DoRunGC() {
  Status s;

  std::unique_ptr<BlobFileMergeIterator> gc_iter;
  s = BuildIterator(&gc_iter);
  if (!s.ok()) return s;
  if (!gc_iter) return Status::Aborted("Build iterator for gc failed");

  // Similar to OptimisticTransaction, we obtain latest_seq from
  // base DB, which is guaranteed to be no smaller than the sequence of
  // current key. We use a WriteCallback on write to check the key sequence
  // on write. If the key sequence is larger than latest_seq, we know
  // a new versions is inserted and the old blob can be discard.
  //
  // We cannot use OptimisticTransaction because we need to pass
  // is_blob_index flag to GetImpl.
  std::unique_ptr<BlobFileHandle> blob_file_handle;
  std::unique_ptr<BlobFileBuilder> blob_file_builder;

  auto* cfh = blob_gc_->column_family_handle();

  //  uint64_t drop_entry_num = 0;
  //  uint64_t drop_entry_size = 0;
  //  uint64_t total_entry_num = 0;
  //  uint64_t total_entry_size = 0;

  uint64_t file_size = 0;

  std::string last_key;
  bool last_key_valid = false;
  gc_iter->SeekToFirst();
  assert(gc_iter->Valid());
  for (; gc_iter->Valid(); gc_iter->Next()) {
    if (IsShutingDown()) {
      s = Status::ShutdownInProgress();
      break;
    }
    BlobIndex blob_index = gc_iter->GetBlobIndex();
    // count read bytes for blob record of gc candidate files
    metrics_.blob_db_bytes_read += blob_index.blob_handle.size;

    if (!last_key.empty() && !gc_iter->key().compare(last_key)) {
      if (last_key_valid) {
        continue;
      }
    } else {
      last_key = gc_iter->key().ToString();
      last_key_valid = false;
    }

    bool discardable = false;
    s = DiscardEntry(gc_iter->key(), blob_index, &discardable);
    if (!s.ok()) {
      break;
    }
    if (discardable) {
      metrics_.blob_db_gc_num_keys_overwritten++;
      metrics_.blob_db_gc_bytes_overwritten += blob_index.blob_handle.size;
      continue;
    }

    last_key_valid = true;

    // Rewrite entry to new blob file
    if ((!blob_file_handle && !blob_file_builder) ||
        file_size >= blob_gc_->titan_cf_options().blob_file_target_size) {
      if (file_size >= blob_gc_->titan_cf_options().blob_file_target_size) {
        assert(blob_file_builder);
        assert(blob_file_handle);
        assert(blob_file_builder->status().ok());
        blob_file_builders_.emplace_back(std::make_pair(
            std::move(blob_file_handle), std::move(blob_file_builder)));
      }
      s = blob_file_manager_->NewFile(&blob_file_handle);
      if (!s.ok()) {
        break;
      }
      ROCKS_LOG_INFO(db_options_.info_log,
                     "Titan new GC output file %" PRIu64 ".",
                     blob_file_handle->GetNumber());
      blob_file_builder = std::unique_ptr<BlobFileBuilder>(
          new BlobFileBuilder(db_options_, blob_gc_->titan_cf_options(),
                              blob_file_handle->GetFile()));
      file_size = 0;
    }
    assert(blob_file_handle);
    assert(blob_file_builder);

    BlobRecord blob_record;
    blob_record.key = gc_iter->key();
    blob_record.value = gc_iter->value();
    // count written bytes for new blob record,
    // blob index's size is counted in `RewriteValidKeyToLSM`
    metrics_.blob_db_bytes_written +=
        blob_record.key.size() + blob_record.value.size();

    BlobIndex new_blob_index;
    new_blob_index.file_number = blob_file_handle->GetNumber();
    blob_file_builder->Add(blob_record, &new_blob_index.blob_handle);
    std::string index_entry;
    new_blob_index.EncodeTo(&index_entry);

    // Store WriteBatch for rewriting new Key-Index pairs to LSM
    GarbageCollectionWriteCallback callback(cfh, blob_record.key.ToString(),
                                            std::move(blob_index));
    callback.value = index_entry;
    rewrite_batches_.emplace_back(
        std::make_pair(WriteBatch(), std::move(callback)));
    auto& wb = rewrite_batches_.back().first;
    s = WriteBatchInternal::PutBlobIndex(&wb, cfh->GetID(), blob_record.key,
                                         index_entry);
    if (!s.ok()) {
      break;
    }
  }

  if (gc_iter->status().ok() && s.ok()) {
    if (blob_file_builder && blob_file_handle) {
      assert(blob_file_builder->status().ok());
      blob_file_builders_.emplace_back(std::make_pair(
          std::move(blob_file_handle), std::move(blob_file_builder)));
    } else {
      assert(!blob_file_builder);
      assert(!blob_file_handle);
    }
  } else if (!gc_iter->status().ok()) {
    return gc_iter->status();
  }

  return s;
}

Status BlobGCJob::BuildIterator(
    std::unique_ptr<BlobFileMergeIterator>* result) {
  Status s;
  const auto& inputs = blob_gc_->gc_sample_inputs();
  assert(!inputs.empty());
  std::vector<std::unique_ptr<BlobFileIterator>> list;
  for (std::size_t i = 0; i < inputs.size(); ++i) {
    std::unique_ptr<PosixRandomRWFile> file;
    // TODO(@DorianZheng) set read ahead size
    s = OpenBlobFile(inputs[i]->file_number(), 0, db_options_, env_options_,
                     env_, &file);
    if (!s.ok()) {
      break;
    }
    list.emplace_back(std::unique_ptr<BlobFileIterator>(new BlobFileIterator(
        std::move(file), inputs[i]->file_number(), inputs[i]->file_size(),
        blob_gc_->titan_cf_options())));
  }

  if (s.ok()) result->reset(new BlobFileMergeIterator(std::move(list)));

  return s;
}

Status BlobGCJob::DiscardEntry(const Slice& key, const BlobIndex& blob_index,
                               bool* discardable) {
  assert(discardable != nullptr);
  PinnableSlice index_entry;
  bool is_blob_index = false;
  Status s = base_db_impl_->GetImpl(
      ReadOptions(), blob_gc_->column_family_handle(), key, &index_entry,
      nullptr /*value_found*/, nullptr /*read_callback*/, &is_blob_index);
  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }
  // count read bytes for checking LSM entry
  metrics_.blob_db_bytes_read += key.size() + index_entry.size();
  if (s.IsNotFound() || !is_blob_index) {
    // Either the key is deleted or updated with a newer version which is
    // inlined in LSM.
    *discardable = true;
    return Status::OK();
  }

  BlobIndex other_blob_index;
  s = other_blob_index.DecodeFrom(&index_entry);
  if (!s.ok()) {
    return s;
  }

  *discardable = !(blob_index == other_blob_index);
  return Status::OK();
}

// We have to make sure crash consistency, but LSM db MANIFEST and BLOB db
// MANIFEST are separate, so we need to make sure all new blob file have
// added to db before we rewrite any key to LSM
Status BlobGCJob::Finish() {
  Status s;
  {
    mutex_->Unlock();
    s = InstallOutputBlobFiles();
    if (s.ok()) {
      s = RewriteValidKeyToLSM();
      if (!s.ok()) {
        ROCKS_LOG_ERROR(db_options_.info_log,
                        "[%s] GC job failed to rewrite keys to LSM: %s",
                        blob_gc_->column_family_handle()->GetName().c_str(),
                        s.ToString().c_str());
      }
    } else {
      ROCKS_LOG_ERROR(db_options_.info_log,
                      "[%s] GC job failed to install output blob files: %s",
                      blob_gc_->column_family_handle()->GetName().c_str(),
                      s.ToString().c_str());
    }
    mutex_->Lock();
  }

  // TODO(@DorianZheng) cal discardable size for new blob file

  if (s.ok() && !blob_gc_->GetColumnFamilyData()->IsDropped()) {
    s = DeleteInputBlobFiles();
  }

  return s;
}

Status BlobGCJob::InstallOutputBlobFiles() {
  Status s;
  for (auto& builder : blob_file_builders_) {
    s = builder.second->Finish();
    if (!s.ok()) {
      break;
    }
    metrics_.blob_db_gc_num_new_files++;
  }
  if (s.ok()) {
    std::vector<std::pair<std::shared_ptr<BlobFileMeta>,
                          std::unique_ptr<BlobFileHandle>>>
        files;
    std::string tmp;
    for (auto& builder : this->blob_file_builders_) {
      auto file = std::make_shared<BlobFileMeta>(
          builder.first->GetNumber(), builder.first->GetFile()->GetFileSize());

      if (!tmp.empty()) {
        tmp.append(" ");
      }
      tmp.append(std::to_string(file->file_number()));
      files.emplace_back(std::make_pair(file, std::move(builder.first)));
    }
    ROCKS_LOG_BUFFER(log_buffer_, "[%s] output[%s]",
                     blob_gc_->column_family_handle()->GetName().c_str(),
                     tmp.c_str());
    s = this->blob_file_manager_->BatchFinishFiles(
        blob_gc_->column_family_handle()->GetID(), files);
    if (s.ok()) {
      for (auto& file : files) {
        blob_gc_->AddOutputFile(file.first.get());
      }
    }
  } else {
    std::vector<unique_ptr<BlobFileHandle>> handles;
    std::string to_delete_files;
    for (auto& builder : this->blob_file_builders_) {
      if (!to_delete_files.empty()) {
        to_delete_files.append(" ");
      }
      to_delete_files.append(std::to_string(builder.first->GetNumber()));
      handles.emplace_back(std::move(builder.first));
    }
    ROCKS_LOG_BUFFER(
        log_buffer_,
        "[%s] InstallOutputBlobFiles failed. Delete GC output files: %s",
        blob_gc_->column_family_handle()->GetName().c_str(),
        to_delete_files.c_str());
    s = this->blob_file_manager_->BatchDeleteFiles(handles);
  }
  return s;
}

Status BlobGCJob::RewriteValidKeyToLSM() {
  Status s;
  auto* db_impl = reinterpret_cast<DBImpl*>(this->base_db_);

  WriteOptions wo;
  wo.low_pri = true;
  wo.ignore_missing_column_families = true;
  for (auto& write_batch : this->rewrite_batches_) {
    if (blob_gc_->GetColumnFamilyData()->IsDropped()) {
      s = Status::Aborted("Column family drop");
      break;
    }
    if (IsShutingDown()) {
      s = Status::ShutdownInProgress();
      break;
    }
    s = db_impl->WriteWithCallback(wo, &write_batch.first, &write_batch.second);
    if (s.ok()) {
      // count written bytes for new blob index.
      metrics_.blob_db_bytes_written += write_batch.first.GetDataSize();
      metrics_.blob_db_gc_num_keys_relocated++;
      metrics_.blob_db_gc_bytes_relocated +=
          write_batch.second.blob_record_size();
      // Key is successfully written to LSM.
    } else if (s.IsBusy()) {
      metrics_.blob_db_gc_num_keys_overwritten++;
      metrics_.blob_db_gc_bytes_overwritten +=
          write_batch.second.blob_record_size();
      // The key is overwritten in the meanwhile. Drop the blob record.
    } else {
      // We hit an error.
      break;
    }
    // count read bytes in write callback
    metrics_.blob_db_bytes_read += write_batch.second.read_bytes();
  }
  if (s.IsBusy()) {
    s = Status::OK();
  }

  if (s.ok()) {
    // Flush and sync WAL.
    s = db_impl->FlushWAL(true /*sync*/);
  }

  return s;
}

Status BlobGCJob::DeleteInputBlobFiles() {
  SequenceNumber obsolete_sequence = base_db_impl_->GetLatestSequenceNumber();

  Status s;
  VersionEdit edit;
  edit.SetColumnFamilyID(blob_gc_->column_family_handle()->GetID());
  for (const auto& file : blob_gc_->gc_sample_inputs()) {
    ROCKS_LOG_INFO(db_options_.info_log, "Titan add obsolete file [%llu]",
                   file->file_number());
    metrics_.blob_db_gc_num_files++;
    edit.DeleteBlobFile(file->file_number(), obsolete_sequence);
  }
  s = version_set_->LogAndApply(edit);
  // TODO(@DorianZheng) Purge pending outputs
  // base_db_->pending_outputs_.erase(handle->GetNumber());
  return s;
}

bool BlobGCJob::IsShutingDown() {
  return (shuting_down_ && shuting_down_->load(std::memory_order_acquire));
}

Status BlobGCJob::DigHole() {
  const auto& inputs = blob_gc_->fs_sample_inputs();
  assert(!inputs.empty());
  return dig_hole_job_->Exec(inputs);
}

}  // namespace titandb
}  // namespace rocksdb
