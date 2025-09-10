#pragma once

#include <cstdint>
#include <optional>
#include <string_view>
#include <utility>

#include <sparrow/record_batch.hpp>

#include "Schema_generated.h"
#include "sparrow_ipc/config/config.hpp"

namespace sparrow_ipc::utils
{
    // Aligns a value to the next multiple of 8, as required by the Arrow IPC format for message bodies
    SPARROW_IPC_API int64_t align_to_8(const int64_t n);

    // Creates a Flatbuffers type from a format string
    // This function maps a sparrow data type to the corresponding Flatbuffers type
    SPARROW_IPC_API std::pair<org::apache::arrow::flatbuf::Type, flatbuffers::Offset<void>>
    get_flatbuffer_type(flatbuffers::FlatBufferBuilder& builder, std::string_view format_str);

    template <std::ranges::input_range R>
        requires std::same_as<std::ranges::range_value_t<R>, sparrow::record_batch>
    SPARROW_IPC_API bool check_record_batches_consistency(const R& record_batches);

    size_t calculate_output_serialized_size(const sparrow::record_batch& record_batch);
}
