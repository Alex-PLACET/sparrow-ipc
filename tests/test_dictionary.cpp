#include <filesystem>
#include <fstream>
#include <iterator>
#include <vector>

#include "doctest/doctest.h"

#include "sparrow_ipc/deserialize.hpp"
#include "sparrow_ipc/flatbuffer_utils.hpp"
#include "sparrow_ipc/memory_output_stream.hpp"
#include "sparrow_ipc/serializer.hpp"
#include "sparrow_ipc/stream_file_serializer.hpp"

namespace
{
    const std::filesystem::path arrow_testing_data_dir = ARROW_TESTING_DATA_DIR;

    const std::filesystem::path dictionary_fixture_base =
        arrow_testing_data_dir / "data" / "arrow-ipc-stream" / "integration" / "cpp-21.0.0" / "generated_dictionary";

    std::vector<uint8_t> read_binary_file(const std::filesystem::path& file_path)
    {
        std::ifstream file(file_path, std::ios::binary);
        REQUIRE(file.is_open());
        return std::vector<uint8_t>((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
    }
}

TEST_SUITE("Dictionary support")
{
    TEST_CASE("Deserializes dictionary fixture stream")
    {
        std::filesystem::path stream_file = dictionary_fixture_base;
        stream_file.replace_extension(".stream");
        const auto stream_data = read_binary_file(stream_file);

        const auto batches = sparrow_ipc::deserialize_stream(std::span<const uint8_t>(stream_data));

        REQUIRE_FALSE(batches.empty());
        REQUIRE_EQ(batches.front().nb_columns(), size_t{3});
        REQUIRE_EQ(batches.front().nb_rows(), size_t{7});
    }

    TEST_CASE("Dictionary stream round-trip preserves values")
    {
        std::filesystem::path stream_file = dictionary_fixture_base;
        stream_file.replace_extension(".stream");
        const auto stream_data = read_binary_file(stream_file);

        const auto input_batches = sparrow_ipc::deserialize_stream(std::span<const uint8_t>(stream_data));

        std::vector<uint8_t> serialized_data;
        sparrow_ipc::memory_output_stream output_stream(serialized_data);
        sparrow_ipc::serializer serializer(output_stream);
        serializer << input_batches << sparrow_ipc::end_stream;

        const auto roundtrip_batches = sparrow_ipc::deserialize_stream(std::span<const uint8_t>(serialized_data));

        REQUIRE_EQ(input_batches.size(), roundtrip_batches.size());
        for (size_t batch_idx = 0; batch_idx < input_batches.size(); ++batch_idx)
        {
            const auto& lhs = input_batches[batch_idx];
            const auto& rhs = roundtrip_batches[batch_idx];
            REQUIRE_EQ(lhs.nb_columns(), rhs.nb_columns());
            REQUIRE_EQ(lhs.nb_rows(), rhs.nb_rows());

            for (size_t col_idx = 0; col_idx < lhs.nb_columns(); ++col_idx)
            {
                const auto& left_col = lhs.get_column(col_idx);
                const auto& right_col = rhs.get_column(col_idx);
                REQUIRE_EQ(left_col.size(), right_col.size());
                REQUIRE_EQ(left_col.data_type(), right_col.data_type());

                for (size_t row_idx = 0; row_idx < left_col.size(); ++row_idx)
                {
                    CHECK_EQ(left_col[row_idx], right_col[row_idx]);
                }
            }
        }
    }

    TEST_CASE("File footer contains dictionary blocks")
    {
        std::filesystem::path stream_file = dictionary_fixture_base;
        stream_file.replace_extension(".stream");
        const auto stream_data = read_binary_file(stream_file);

        const auto batches = sparrow_ipc::deserialize_stream(std::span<const uint8_t>(stream_data));

        std::vector<uint8_t> file_data;
        sparrow_ipc::memory_output_stream output_stream(file_data);
        sparrow_ipc::stream_file_serializer file_serializer(output_stream);
        file_serializer << batches << sparrow_ipc::end_file;

        const auto* footer = sparrow_ipc::get_footer_from_file_data(std::span<const uint8_t>(file_data));
        REQUIRE_NE(footer, nullptr);
        REQUIRE_NE(footer->dictionaries(), nullptr);
        CHECK_GT(footer->dictionaries()->size(), 0);
    }
}
