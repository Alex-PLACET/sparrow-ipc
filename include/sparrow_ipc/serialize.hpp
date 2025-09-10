#pragma once

#include <ostream>
#include <ranges>
#include <vector>

#include <sparrow/record_batch.hpp>

#include "Message_generated.h"

namespace sparrow_ipc
{
    template <std::ranges::input_range R>
        requires std::same_as<std::ranges::range_value_t<R>, sparrow::record_batch>
    std::vector<uint8_t> serialize(const R& record_batches);

    std::vector<uint8_t> serialize_schema_message(const ArrowSchema& arrow_schema);

    std::vector<uint8_t> serialize_record_batch(const sparrow::record_batch& record_batch);

    template <std::ranges::input_range R>
        requires std::same_as<std::ranges::range_value_t<R>, sparrow::record_batch>
    std::vector<uint8_t> serialize_record_batches(const R& record_batches);

    flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<org::apache::arrow::flatbuf::KeyValue>>>
    create_metadata(flatbuffers::FlatBufferBuilder& builder, const ArrowSchema& arrow_schema);

    ::flatbuffers::Offset<org::apache::arrow::flatbuf::Field>
    create_field(flatbuffers::FlatBufferBuilder& builder, const ArrowSchema& arrow_schema);

    ::flatbuffers::Offset<::flatbuffers::Vector<::flatbuffers::Offset<org::apache::arrow::flatbuf::Field>>>
    create_children(flatbuffers::FlatBufferBuilder& builder, const ArrowSchema& arrow_schema);

    flatbuffers::FlatBufferBuilder get_schema_message_builder(const ArrowSchema& arrow_schema);
    std::vector<uint8_t> serialize_schema_message(const ArrowSchema& arrow_schema);

    void fill_fieldnodes(
        const sparrow::arrow_proxy& arrow_proxy,
        std::vector<org::apache::arrow::flatbuf::FieldNode>& nodes
    );

    std::vector<org::apache::arrow::flatbuf::FieldNode>
    create_fieldnodes(const sparrow::record_batch& record_batch);

    void fill_buffers(
        const sparrow::arrow_proxy& arrow_proxy,
        std::vector<org::apache::arrow::flatbuf::Buffer>& flatbuf_buffers,
        int64_t& offset
    );

    std::vector<org::apache::arrow::flatbuf::Buffer> get_buffers(const sparrow::record_batch& record_batch);

    void fill_body(const sparrow::arrow_proxy& arrow_proxy, std::vector<uint8_t>& body);
    std::vector<uint8_t> generate_body(const sparrow::record_batch& record_batch);
    int64_t calculate_body_size(const sparrow::arrow_proxy& arrow_proxy);
    int64_t calculate_body_size(const sparrow::record_batch& record_batch);

    flatbuffers::FlatBufferBuilder get_record_batch_message_builder(
        const sparrow::record_batch& record_batch,
        const std::vector<org::apache::arrow::flatbuf::FieldNode>& nodes,
        const std::vector<org::apache::arrow::flatbuf::Buffer>& buffers
    );

    std::vector<uint8_t> serialize_record_batch(const sparrow::record_batch& record_batch);
}
