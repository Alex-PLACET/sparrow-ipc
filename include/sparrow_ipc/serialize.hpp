#pragma once

#include <ostream>
#include <ranges>

#include <sparrow/record_batch.hpp>

namespace sparrow_ipc
{
    template <std::ranges::input_range R>
        requires std::same_as<std::ranges::range_value_t<R>, sparrow::record_batch>
    void serialize(const R& record_batches, std::ostream& out);

    std::vector<uint8_t> serialize_schema_message(const ArrowSchema& arrow_schema);

    template <std::ranges::input_range R>
        requires std::same_as<std::ranges::range_value_t<R>, sparrow::record_batch>
    std::vector<uint8_t> serialize_record_batches(const R& record_batches);


}
