#include "blob_file_builder.h"
#include "blob_file_cache.h"
#include "blob_file_reader.h"
#include "util/filename.h"
#include "util/testharness.h"
#include "util/testutil.h"

namespace rocksdb {
namespace titandb {

class BlobFileTest : public testing::Test {
 public:
  BlobFileTest() : dirname_(test::TmpDir(env_)) {
    file_name_ = BlobFileName(dirname_, file_number_);
  }

  ~BlobFileTest() {
    env_->DeleteFile(file_name_);
    env_->DeleteDir(dirname_);
  }

  void TestBlobFilePrefetcher(TitanOptions options) {
    options.dirname = dirname_;
    TitanDBOptions db_options(options);
    TitanCFOptions cf_options(options);
    BlobFileCache cache(db_options, cf_options, {NewLRUCache(128)}, nullptr);

    const int n = 100;
    std::vector<BlobHandle> handles(n);

    std::unique_ptr<WritableFileWriter> file;
    {
      std::unique_ptr<WritableFile> f;
      ASSERT_OK(env_->NewWritableFile(file_name_, &f, env_options_));
      file.reset(
          new WritableFileWriter(std::move(f), file_name_, env_options_));
    }
    std::unique_ptr<BlobFileBuilder> builder(
        new BlobFileBuilder(db_options, cf_options, file.get()));

    for (int i = 0; i < n; i++) {
      auto key = std::to_string(i);
      auto value = std::string(1024, i);
      BlobRecord record;
      record.key = key;
      record.value = value;
      builder->Add(record, &handles[i]);
      ASSERT_OK(builder->status());
    }
    ASSERT_OK(builder->Finish());
    ASSERT_OK(builder->status());

    uint64_t file_size = 0;
    ASSERT_OK(env_->GetFileSize(file_name_, &file_size));

    ReadOptions ro;
    std::unique_ptr<BlobFilePrefetcher> prefetcher;
    ASSERT_OK(cache.NewPrefetcher(file_number_, file_size, &prefetcher));
    for (int i = 0; i < n; i++) {
      auto key = std::to_string(i);
      auto value = std::string(1024, i);
      BlobRecord expect;
      expect.key = key;
      expect.value = value;
      BlobRecord record;
      PinnableSlice buffer;
      ASSERT_OK(
          cache.Get(ro, file_number_, file_size, handles[i], &record, &buffer));
      ASSERT_EQ(record, expect);
      buffer.Reset();
      ASSERT_OK(
          cache.Get(ro, file_number_, file_size, handles[i], &record, &buffer));
      ASSERT_EQ(record, expect);
      buffer.Reset();
      ASSERT_OK(prefetcher->Get(ro, handles[i], &record, &buffer));
      ASSERT_EQ(record, expect);
      buffer.Reset();
      ASSERT_OK(prefetcher->Get(ro, handles[i], &record, &buffer));
      ASSERT_EQ(record, expect);
    }
  }

  void TestBlobFileReader(TitanOptions options) {
    options.dirname = dirname_;
    TitanDBOptions db_options(options);
    TitanCFOptions cf_options(options);
    BlobFileCache cache(db_options, cf_options, {NewLRUCache(128)}, nullptr);

    const int n = 100;
    std::vector<BlobHandle> handles(n);

    std::unique_ptr<WritableFileWriter> file;
    {
      std::unique_ptr<WritableFile> f;
      ASSERT_OK(env_->NewWritableFile(file_name_, &f, env_options_));
      file.reset(
          new WritableFileWriter(std::move(f), file_name_, env_options_));
    }
    std::unique_ptr<BlobFileBuilder> builder(
        new BlobFileBuilder(db_options, cf_options, file.get()));

    for (int i = 0; i < n; i++) {
      auto key = std::to_string(i);
      auto value = std::string(1024, i);
      BlobRecord record;
      record.key = key;
      record.value = value;
      builder->Add(record, &handles[i]);
      ASSERT_OK(builder->status());
    }
    ASSERT_OK(builder->Finish());
    ASSERT_OK(builder->status());

    uint64_t file_size = 0;
    ASSERT_OK(env_->GetFileSize(file_name_, &file_size));

    ReadOptions ro;
    std::unique_ptr<RandomAccessFileReader> random_access_file_reader;
    ASSERT_OK(NewBlobFileReader(file_number_, 0, db_options, env_options_, env_,
                                &random_access_file_reader));
    std::unique_ptr<BlobFileReader> blob_file_reader;
    ASSERT_OK(BlobFileReader::Open(cf_options,
                                   std::move(random_access_file_reader),
                                   file_size, &blob_file_reader, nullptr));
    for (int i = 0; i < n; i++) {
      auto key = std::to_string(i);
      auto value = std::string(1024, i);
      BlobRecord expect;
      expect.key = key;
      expect.value = value;
      BlobRecord record;
      PinnableSlice buffer;
      ASSERT_OK(
          cache.Get(ro, file_number_, file_size, handles[i], &record, &buffer));
      ASSERT_EQ(record, expect);
      buffer.Reset();
      ASSERT_OK(
          cache.Get(ro, file_number_, file_size, handles[i], &record, &buffer));
      ASSERT_EQ(record, expect);
      buffer.Reset();
      ASSERT_OK(blob_file_reader->Get(ro, handles[i], &record, &buffer));
      ASSERT_EQ(record, expect);
      buffer.Reset();
      ASSERT_OK(blob_file_reader->Get(ro, handles[i], &record, &buffer));
      ASSERT_EQ(record, expect);
    }
  }

  void TestBlobFile4KAlign(TitanOptions options) {
    options.dirname = dirname_;
    TitanDBOptions db_options(options);
    TitanCFOptions cf_options(options);
    BlobFileCache cache(db_options, cf_options, {NewLRUCache(128)}, nullptr);
    const uint64_t kBlockSize = 4096;

    const int n = 1000;
    std::vector<BlobHandle> handles(n);

    std::unique_ptr<WritableFileWriter> file;
    {
      std::unique_ptr<WritableFile> f;
      ASSERT_OK(env_->NewWritableFile(file_name_, &f, env_options_));
      file.reset(
          new WritableFileWriter(std::move(f), file_name_, env_options_));
    }
    std::unique_ptr<BlobFileBuilder> builder(
        new BlobFileBuilder(db_options, cf_options, file.get()));

    Random rnd(301);
    for (int i = 0; i < n; i++) {
      auto key = std::to_string(i);
      std::string value;
      test::RandomString(&rnd, rnd.Skewed(20) + 1024, &value);
      BlobRecord record;
      record.key = key;
      record.value = value;
      builder->Add(record, &handles[i]);
      ASSERT_OK(builder->status());
    }
    ASSERT_OK(builder->Finish());
    ASSERT_OK(builder->status());

    ASSERT_TRUE(handles[0].offset % kBlockSize == 0);

    for (int i = 1; i < n; i++) {
      if (handles[i].offset % kBlockSize != 0) {
        ASSERT_TRUE(handles[i].offset + handles[i].size <=
                    (handles[i].offset / kBlockSize + 1) * kBlockSize);
      }
    }
  }

  Env* env_{Env::Default()};
  EnvOptions env_options_;
  std::string dirname_;
  std::string file_name_;
  uint64_t file_number_{1};
};

TEST_F(BlobFileTest, BlobFileReader) {
  TitanOptions options;
  TestBlobFileReader(options);
  options.blob_file_compression = kLZ4Compression;
  TestBlobFileReader(options);
}

TEST_F(BlobFileTest, BlobFilePrefetcher) {
  TitanOptions options;
  TestBlobFilePrefetcher(options);
  options.blob_cache = NewLRUCache(1 << 20);
  TestBlobFilePrefetcher(options);
  options.blob_file_compression = kLZ4Compression;
  TestBlobFilePrefetcher(options);
}

TEST_F(BlobFileTest, BloblFile4KAlign) {
  TitanOptions options;
  TestBlobFile4KAlign(options);
}

}  // namespace titandb
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
