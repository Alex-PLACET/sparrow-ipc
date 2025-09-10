#include "sparrow_ipc/serialize.hpp"

#include <iterator>

#include "Message_generated.h"
#include "sparrow_ipc/magic_values.hpp"
#include "sparrow_ipc/utils.hpp"

namespace sparrow_ipc
{
    ::flatbuffers::Offset<::flatbuffers::Vector<::flatbuffers::Offset<org::apache::arrow::flatbuf::Field>>>
    create_children(flatbuffers::FlatBufferBuilder& builder, const ArrowSchema& arrow_schema)
    {
        std::vector<flatbuffers::Offset<org::apache::arrow::flatbuf::Field>> children_vec;
        children_vec.reserve(arrow_schema.n_children);
        for (int i = 0; i < arrow_schema.n_children; ++i)
        {
            if (arrow_schema.children[i] == nullptr)
            {
                throw std::invalid_argument("ArrowSchema has null child at index " + std::to_string(i));
            }
            flatbuffers::Offset<org::apache::arrow::flatbuf::Field> field = create_field(
                builder,
                *(arrow_schema.children[i])
            );
            children_vec.emplace_back(field);
        }
        return children_vec.empty() ? 0 : builder.CreateVector(children_vec);
    }

    flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<org::apache::arrow::flatbuf::KeyValue>>>
    create_metadata(flatbuffers::FlatBufferBuilder& builder, const ArrowSchema& arrow_schema)
    {
        if (arrow_schema.metadata == nullptr)
        {
            return 0;
        }

        const auto metadata_view = sparrow::key_value_view(arrow_schema.metadata);
        std::vector<flatbuffers::Offset<org::apache::arrow::flatbuf::KeyValue>> kv_offsets;
        kv_offsets.reserve(metadata_view.size());
        for (const auto& [key, value] : metadata_view)
        {
            const auto key_offset = builder.CreateString(std::string(key));
            const auto value_offset = builder.CreateString(std::string(value));
            kv_offsets.push_back(org::apache::arrow::flatbuf::CreateKeyValue(builder, key_offset, value_offset));
        }
        return builder.CreateVector(kv_offsets);
    }

    ::flatbuffers::Offset<org::apache::arrow::flatbuf::Field>
    create_field(flatbuffers::FlatBufferBuilder& builder, const ArrowSchema& arrow_schema)
    {
        flatbuffers::Offset<flatbuffers::String> fb_name_offset = (arrow_schema.name == nullptr)
                                                                      ? 0
                                                                      : builder.CreateString(arrow_schema.name);

        const auto [type_enum, type_offset] = utils::get_flatbuffer_type(builder, arrow_schema.format);
        auto fb_metadata_offset = create_metadata(builder, arrow_schema);
        const auto children = create_children(builder, arrow_schema);

        const auto fb_field = org::apache::arrow::flatbuf::CreateField(
            builder,
            fb_name_offset,
            (arrow_schema.flags & static_cast<int64_t>(sparrow::ArrowFlag::NULLABLE)) != 0,
            type_enum,
            type_offset,
            0,  // TODO: support dictionary
            children,
            fb_metadata_offset
        );
        return fb_field;
    }

    flatbuffers::FlatBufferBuilder get_schema_message_builder(const ArrowSchema& arrow_schema)
    {
        flatbuffers::FlatBufferBuilder schema_builder;
        const auto fields_vec = create_children(schema_builder, arrow_schema);
        const auto schema_offset = org::apache::arrow::flatbuf::CreateSchema(
            schema_builder,
            org::apache::arrow::flatbuf::Endianness::Little,  // TODO: make configurable
            fields_vec
        );
        const auto schema_message_offset = org::apache::arrow::flatbuf::CreateMessage(
            schema_builder,
            org::apache::arrow::flatbuf::MetadataVersion::V5,
            org::apache::arrow::flatbuf::MessageHeader::Schema,
            schema_offset.Union(),
            0  // body length IS 0 for schema messages
        );
        schema_builder.Finish(schema_message_offset);
        return schema_builder;
    }

    std::vector<uint8_t> serialize_schema_message(const ArrowSchema& arrow_schema)
    {
        std::vector<uint8_t> schema_buffer;

        schema_buffer.insert(schema_buffer.end(), continuation.begin(), continuation.end());
        flatbuffers::FlatBufferBuilder schema_builder = get_schema_message_builder(arrow_schema);
        const flatbuffers::uoffset_t schema_len = schema_builder.GetSize();
        schema_buffer.reserve(schema_buffer.size() + sizeof(uint32_t) + schema_len);
        // Write the 4-byte length prefix after the continuation bytes
        schema_buffer.insert(
            schema_buffer.end(),
            reinterpret_cast<const uint8_t*>(&schema_len),
            reinterpret_cast<const uint8_t*>(&schema_len) + sizeof(uint32_t)
        );
        // Append the actual message bytes
        schema_buffer.insert(
            schema_buffer.end(),
            schema_builder.GetBufferPointer(),
            schema_builder.GetBufferPointer() + schema_len
        );
        // padding to 8 bytes
        schema_buffer.insert(
            schema_buffer.end(),
            utils::align_to_8(static_cast<int64_t>(schema_buffer.size()))
                - static_cast<int64_t>(schema_buffer.size()),
            0
        );
        return schema_buffer;
    }

    void fill_fieldnodes(
        const sparrow::arrow_proxy& arrow_proxy,
        std::vector<org::apache::arrow::flatbuf::FieldNode>& nodes
    )
    {
        nodes.emplace_back(arrow_proxy.length(), arrow_proxy.null_count());
        nodes.reserve(nodes.size() + arrow_proxy.n_children());
        for (const auto& child : arrow_proxy.children())
        {
            fill_fieldnodes(child, nodes);
        }
    }

    std::vector<org::apache::arrow::flatbuf::FieldNode>
    create_fieldnodes(const sparrow::record_batch& record_batch)
    {
        std::vector<org::apache::arrow::flatbuf::FieldNode> nodes;
        nodes.reserve(record_batch.columns().size());
        for (const auto& column : record_batch.columns())
        {
            fill_fieldnodes(sparrow::detail::array_access::get_arrow_proxy(column), nodes);
        }
        return nodes;
    }

    void fill_buffers(
        const sparrow::arrow_proxy& arrow_proxy,
        std::vector<org::apache::arrow::flatbuf::Buffer>& flatbuf_buffers,
        int64_t& offset
    )
    {
        const auto& buffers = arrow_proxy.buffers();
        for (const auto& buffer : buffers)
        {
            int64_t size = static_cast<int64_t>(buffer.size());
            flatbuf_buffers.emplace_back(offset, size);
            offset += utils::align_to_8(size);
        }
        for (const auto& child : arrow_proxy.children())
        {
            const auto& child_arrow_proxy = sparrow::detail::array_access::get_arrow_proxy(child);
            fill_buffers(child_arrow_proxy, flatbuf_buffers, offset);
        }
    }

    std::vector<org::apache::arrow::flatbuf::Buffer> get_buffers(const sparrow::record_batch& record_batch)
    {
        std::vector<org::apache::arrow::flatbuf::Buffer> buffers;
        std::int64_t offset = 0;
        for (const auto& column : record_batch.columns())
        {
            const auto& arrow_proxy = sparrow::detail::array_access::get_arrow_proxy(column);
            fill_buffers(arrow_proxy, buffers, offset);
        }
        return buffers;
    }

    void fill_body(const sparrow::arrow_proxy& arrow_proxy, std::vector<uint8_t>& body)
    {
        for (const auto& buffer : arrow_proxy.buffers())
        {
            body.insert(body.end(), buffer.begin(), buffer.end());
            const int64_t padding_size = utils::align_to_8(static_cast<int64_t>(buffer.size()))
                                         - static_cast<int64_t>(buffer.size());
            body.insert(body.end(), padding_size, 0);
        }
        for (const auto& child : arrow_proxy.children())
        {
            const auto& child_arrow_proxy = sparrow::detail::array_access::get_arrow_proxy(child);
            fill_body(child_arrow_proxy, body);
        }
    }

    std::vector<uint8_t> generate_body(const sparrow::record_batch& record_batch)
    {
        std::vector<uint8_t> body;
        for (const auto& column : record_batch.columns())
        {
            const auto& arrow_proxy = sparrow::detail::array_access::get_arrow_proxy(column);
            fill_body(arrow_proxy, body);
        }
        return body;
    }

    std::vector<uint8_t> serialize_record_batch(const sparrow::record_batch& record_batch)
    {
        std::vector<org::apache::arrow::flatbuf::FieldNode> nodes = create_fieldnodes(record_batch);
        std::vector<org::apache::arrow::flatbuf::Buffer> flatbuf_buffers = get_buffers(record_batch);
        flatbuffers::FlatBufferBuilder record_batch_builder;
        org::apache::arrow::flatbuf::CreateRecordBatchDirect(
            record_batch_builder,
            static_cast<int64_t>(record_batch.nb_rows()),
            &nodes,
            &flatbuf_buffers
        );
        std::vector<uint8_t> output;
        output.insert(output.end(), continuation.begin(), continuation.end());
        const flatbuffers::uoffset_t record_batch_len = record_batch_builder.GetSize();
        output.insert(
            output.end(),
            reinterpret_cast<const uint8_t*>(&record_batch_len),
            reinterpret_cast<const uint8_t*>(&record_batch_len) + sizeof(record_batch_len)
        );
        output.insert(
            output.end(),
            record_batch_builder.GetBufferPointer(),
            record_batch_builder.GetBufferPointer() + record_batch_len
        );
        // padding to 8 bytes
        output.insert(
            output.end(),
            utils::align_to_8(static_cast<int64_t>(output.size())) - static_cast<int64_t>(output.size()),
            0
        );
        std::vector<uint8_t> body = generate_body(record_batch);
        output.insert(output.end(), std::make_move_iterator(body.begin()), std::make_move_iterator(body.end()));
        return output;
    }

    template <std::ranges::input_range R>
        requires std::same_as<std::ranges::range_value_t<R>, sparrow::record_batch>
    std::vector<uint8_t> serialize_record_batches(const R& record_batches)
    {
        std::vector<uint8_t> output;
        for (const auto& record_batch : record_batches)
        {
            const auto rb_serialized = serialize_record_batch(record_batch);
            output.insert(
                output.end(),
                std::make_move_iterator(rb_serialized.begin()),
                std::make_move_iterator(rb_serialized.end())
            );
        }
        return output;
    }

    template <std::ranges::input_range R>
        requires std::same_as<std::ranges::range_value_t<R>, sparrow::record_batch>
    void serialize(const R& record_batches, std::ostream& out)
    {
        if (check_record_batches_consistency(record_batches))
        {
            throw std::invalid_argument(
                "All record batches must have the same schema to be serialized together."
            );
        }
        std::vector<uint8_t> serialized_schema = serialize_schema_message(record_batches[0].schema());
        std::vector<uint8_t> serialized_record_batches = serialize_record_batches(record_batches);
    }
}