#include "table_builder.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>

namespace rocksdb {
namespace titandb {

void TitanTableBuilder::Add(const Slice& key, const Slice& value) {
  if (!ok()) return;

  ParsedInternalKey ikey;
  if (!ParseInternalKey(key, &ikey)) {
    status_ = Status::Corruption(Slice());
    return;
  }

  if (ikey.type == kTypeBlobIndex &&
      cf_options_.blob_run_mode == TitanBlobRunMode::kFallback) {
    // we ingest value from blob file
    Slice copy = value;
    BlobIndex index;
    status_ = index.DecodeFrom(&copy);
    if (!ok()) {
      return;
    }

    BlobRecord record;
    PinnableSlice buffer;

    auto storage = blob_storage_.lock();
    assert(storage != nullptr);

    ReadOptions options;  // dummy option
    Status get_status = storage->Get(options, index, &record, &buffer);
    if (get_status.ok()) {
      ikey.type = kTypeValue;
      std::string index_key;
      AppendInternalKey(&index_key, ikey);
      base_builder_->Add(index_key, record.value);
    } else {
      // Get blob value can fail if corresponding blob file has been GC-ed
      // deleted. In this case we write the blob index as is to compaction
      // output.
      // TODO: return error if it is indeed an error.
      base_builder_->Add(key, value);
    }
  } else if (ikey.type == kTypeValue &&
             value.size() >= cf_options_.min_blob_size &&
             cf_options_.blob_run_mode == TitanBlobRunMode::kNormal) {
    // we write to blob file and insert index
    std::string index_value;
    AddBlob(ikey.user_key, value, &index_value);
    if (ok()) {
      ikey.type = kTypeBlobIndex;
      std::string index_key;
      AppendInternalKey(&index_key, ikey);
      base_builder_->Add(index_key, index_value);
    }
  } else {
    base_builder_->Add(key, value);
  }
}

void TitanTableBuilder::AddBlob(const Slice& key, const Slice& value,
                                std::string* index_value) {
  if (!ok()) return;
  StopWatch write_sw(db_options_.env, statistics(stats_),
                     BLOB_DB_BLOB_FILE_WRITE_MICROS);

  if (!blob_builder_) {
    status_ = blob_manager_->NewFile(&blob_handle_);
    if (!ok()) return;
    ROCKS_LOG_INFO(db_options_.info_log,
                   "Titan table builder created new blob file %" PRIu64 ".",
                   blob_handle_->GetNumber());
    blob_builder_.reset(
        new BlobFileBuilder(db_options_, cf_options_, blob_handle_->GetFile()));
  }

  RecordTick(stats_, BLOB_DB_NUM_KEYS_WRITTEN);
  MeasureTime(stats_, BLOB_DB_KEY_SIZE, key.size());
  MeasureTime(stats_, BLOB_DB_VALUE_SIZE, value.size());
  AddStats(stats_, cf_id_, TitanInternalStats::LIVE_BLOB_SIZE, value.size());

  BlobIndex index;
  BlobRecord record;
  record.key = key;
  record.value = value;
  index.file_number = blob_handle_->GetNumber();
  blob_builder_->Add(record, &index.blob_handle);
  RecordTick(stats_, BLOB_DB_BLOB_FILE_BYTES_WRITTEN, index.blob_handle.size);
  if (ok()) {
    index.EncodeTo(index_value);
  }
}

Status TitanTableBuilder::status() const {
  Status s = status_;
  if (s.ok()) {
    s = base_builder_->status();
  }
  if (s.ok() && blob_builder_) {
    s = blob_builder_->status();
  }
  return s;
}

Status TitanTableBuilder::Finish() {
  base_builder_->Finish();
  if (blob_builder_) {
    blob_builder_->Finish();
    if (ok()) {
      ROCKS_LOG_INFO(db_options_.info_log,
                     "Titan table builder finish output file %" PRIu64 ".",
                     blob_handle_->GetNumber());
      std::shared_ptr<BlobFileMeta> file = std::make_shared<BlobFileMeta>(
          blob_handle_->GetNumber(), blob_handle_->GetFile()->GetFileSize());
      const uint64_t kBlockSize = 4096;
      file->set_real_file_size(
          (file->file_size() - 1) / kBlockSize * kBlockSize + kBlockSize);
      file->FileStateTransit(BlobFileMeta::FileEvent::kFlushOrCompactionOutput);
      status_ =
          blob_manager_->FinishFile(cf_id_, file, std::move(blob_handle_));
    } else {
      ROCKS_LOG_WARN(
          db_options_.info_log,
          "Titan table builder finish failed. Delete output file %" PRIu64 ".",
          blob_handle_->GetNumber());
      status_ = blob_manager_->DeleteFile(std::move(blob_handle_));
    }
  }
  if (!status_.ok()) {
    ROCKS_LOG_ERROR(db_options_.info_log,
                    "Titan table builder failed on finish: %s",
                    status_.ToString().c_str());
  }
  return status();
}

void TitanTableBuilder::Abandon() {
  base_builder_->Abandon();
  if (blob_builder_) {
    ROCKS_LOG_INFO(db_options_.info_log,
                   "Titan table builder abandoned. Delete output file %" PRIu64
                   ".",
                   blob_handle_->GetNumber());
    blob_builder_->Abandon();
    status_ = blob_manager_->DeleteFile(std::move(blob_handle_));
  }
}

uint64_t TitanTableBuilder::NumEntries() const {
  return base_builder_->NumEntries();
}

uint64_t TitanTableBuilder::FileSize() const {
  return base_builder_->FileSize();
}

bool TitanTableBuilder::NeedCompact() const {
  return base_builder_->NeedCompact();
}

TableProperties TitanTableBuilder::GetTableProperties() const {
  return base_builder_->GetTableProperties();
}

}  // namespace titandb
}  // namespace rocksdb
