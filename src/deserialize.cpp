#include "sparrow_ipc/deserialize.hpp"

#include <memory>
#include <unordered_map>

#include "array_deserializer.hpp"
#include <sparrow/arrow_interface/arrow_array.hpp>
#include <sparrow/arrow_interface/arrow_schema.hpp>
#include "sparrow_ipc/deserialize_primitive_array.hpp"
#include "sparrow_ipc/dictionary_cache.hpp"
#include "sparrow_ipc/encapsulated_message.hpp"
#include "sparrow_ipc/magic_values.hpp"
#include "sparrow_ipc/metadata.hpp"

namespace sparrow_ipc
{
    namespace
    {
        // End-of-stream marker size in bytes
        constexpr size_t END_OF_STREAM_MARKER_SIZE = 8;
        constexpr int DICTIONARY_INDEX_BIT_WIDTH_8 = 8;
        constexpr int DICTIONARY_INDEX_BIT_WIDTH_16 = 16;
        constexpr int DICTIONARY_INDEX_BIT_WIDTH_32 = 32;
        constexpr int DICTIONARY_INDEX_BIT_WIDTH_64 = 64;

        void collect_dictionary_fields(
            const org::apache::arrow::flatbuf::Field& field,
            std::unordered_map<int64_t, const org::apache::arrow::flatbuf::Field*>& dictionary_fields
        )
        {
            if (const auto* dictionary = field.dictionary(); dictionary != nullptr)
            {
                dictionary_fields[dictionary->id()] = &field;
            }

            const auto* children = field.children();
            if (children == nullptr)
            {
                return;
            }

            for (const auto* child : *children)
            {
                if (child != nullptr)
                {
                    collect_dictionary_fields(*child, dictionary_fields);
                }
            }
        }

        sparrow::array apply_dictionary_encoding(
            sparrow::array index_array,
            const org::apache::arrow::flatbuf::Field& field,
            const dictionary_cache& dictionaries
        )
        {
            const auto* dictionary_encoding = field.dictionary();
            if (dictionary_encoding == nullptr)
            {
                return index_array;
            }

            const auto dictionary_batch = dictionaries.get_dictionary(dictionary_encoding->id());
            if (!dictionary_batch.has_value())
            {
                throw std::runtime_error(
                    "Dictionary with id " + std::to_string(dictionary_encoding->id())
                    + " not found when decoding dictionary-encoded field"
                );
            }

            const auto& dictionary_columns = dictionary_batch->get().columns();
            if (dictionary_columns.size() != 1)
            {
                throw std::runtime_error("Dictionary batch must have exactly one column");
            }

            auto [index_arrow_array, index_arrow_schema] = sparrow::extract_arrow_structures(std::move(index_array));

            const auto& dictionary_proxy = sparrow::detail::array_access::get_arrow_proxy(
                dictionary_columns.front()
            );
            auto dictionary_arrow_array = sparrow::copy_array(
                dictionary_proxy.array(),
                dictionary_proxy.schema()
            );
            auto dictionary_arrow_schema = sparrow::copy_schema(dictionary_proxy.schema());

            auto dictionary_array_ptr = std::make_unique<ArrowArray>(dictionary_arrow_array);
            auto dictionary_schema_ptr = std::make_unique<ArrowSchema>(dictionary_arrow_schema);
            index_arrow_array.dictionary = dictionary_array_ptr.release();
            index_arrow_schema.dictionary = dictionary_schema_ptr.release();

            return sparrow::array(ArrowArray(index_arrow_array), ArrowSchema(index_arrow_schema));
        }

        sparrow::record_batch deserialize_dictionary_batch(
            const org::apache::arrow::flatbuf::DictionaryBatch& dictionary_batch,
            const encapsulated_message& encapsulated_message,
            const std::unordered_map<int64_t, const org::apache::arrow::flatbuf::Field*>& dictionary_fields
        )
        {
            const auto* dict_record_batch = dictionary_batch.data();
            if (dict_record_batch == nullptr)
            {
                throw std::runtime_error("DictionaryBatch message has null RecordBatch data");
            }

            const auto field_it = dictionary_fields.find(dictionary_batch.id());
            if (field_it == dictionary_fields.end())
            {
                throw std::runtime_error(
                    "Dictionary with id " + std::to_string(dictionary_batch.id())
                    + " is not declared in schema"
                );
            }

            const auto* field = field_it->second;
            std::optional<std::vector<sparrow::metadata_pair>> metadata;
            if (field->custom_metadata() != nullptr)
            {
                metadata = to_sparrow_metadata(*field->custom_metadata());
            }

            size_t buffer_index = 0;
            size_t node_index = 0;
            size_t variadic_counts_idx = 0;

            const std::string field_name = (field->name() != nullptr && field->name()->size() > 0)
                                               ? field->name()->str()
                                               : std::string("__dictionary__");

            auto values = array_deserializer::deserialize(
                *dict_record_batch,
                encapsulated_message.body(),
                dict_record_batch->length(),
                field_name,
                metadata,
                field->nullable(),
                buffer_index,
                node_index,
                variadic_counts_idx,
                *field
            );

            std::vector<std::string> names;
            names.emplace_back(field_name);
            std::vector<sparrow::array> arrays;
            arrays.emplace_back(std::move(values));
            return sparrow::record_batch(std::move(names), std::move(arrays));
        }

        sparrow::array deserialize_dictionary_indices(
            const org::apache::arrow::flatbuf::RecordBatch& record_batch,
            const std::span<const uint8_t>& body,
            int64_t length,
            const std::string& name,
            const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
            bool nullable,
            size_t& buffer_index,
            size_t& node_index,
            const org::apache::arrow::flatbuf::Field& field
        )
        {
            const auto* dictionary = field.dictionary();
            if (dictionary == nullptr || dictionary->indexType() == nullptr)
            {
                throw std::runtime_error("Dictionary-encoded field is missing indexType");
            }

            ++node_index; // Consume one FieldNode for dictionary indices

            const auto* index_type = dictionary->indexType();
            const auto bit_width = index_type->bitWidth();
            const bool is_signed = index_type->is_signed();

            const auto deserialize_for = [&]<class T>() -> sparrow::array
            {
                return sparrow::array(
                    deserialize_primitive_array<T>(
                        record_batch,
                        body,
                        length,
                        name,
                        metadata,
                        nullable,
                        buffer_index
                    )
                );
            };

            switch (bit_width)
            {
                case DICTIONARY_INDEX_BIT_WIDTH_8:
                    return is_signed ? deserialize_for.template operator()<int8_t>()
                                     : deserialize_for.template operator()<uint8_t>();
                case DICTIONARY_INDEX_BIT_WIDTH_16:
                    return is_signed ? deserialize_for.template operator()<int16_t>()
                                     : deserialize_for.template operator()<uint16_t>();
                case DICTIONARY_INDEX_BIT_WIDTH_32:
                    return is_signed ? deserialize_for.template operator()<int32_t>()
                                     : deserialize_for.template operator()<uint32_t>();
                case DICTIONARY_INDEX_BIT_WIDTH_64:
                    return is_signed ? deserialize_for.template operator()<int64_t>()
                                     : deserialize_for.template operator()<uint64_t>();
                default:
                    throw std::runtime_error(
                        "Unsupported dictionary index bit width: " + std::to_string(bit_width)
                    );
            }
        }
    }

    /**
     * @brief Deserializes arrays from an Apache Arrow RecordBatch using the provided schema.
     *
     * This function processes each field in the schema and deserializes the corresponding
     * data from the RecordBatch into sparrow::array objects. It handles various Arrow data
     * types including primitive types (bool, integers, floating point), binary data, string
     * data, fixed-size binary data, and interval types.
     *
     * @param record_batch The Apache Arrow FlatBuffer RecordBatch containing the serialized data
     * @param schema The Apache Arrow FlatBuffer Schema defining the structure and types of the data
     * @param encapsulated_message The message containing the binary data buffers
     * @param field_metadata Metadata associated with each field in the schema
     *
     * @return std::vector<sparrow::array> A vector of deserialized arrays, one for each field in the schema
     *
     * @throws std::runtime_error If an unsupported data type, integer bit width, floating point precision,
     *         or interval unit is encountered
     *
     * @note The function maintains a buffer index that is incremented as it processes each field
     *       to correctly map data buffers to their corresponding arrays.
     */
    std::vector<sparrow::array> get_arrays_from_record_batch(
        const org::apache::arrow::flatbuf::RecordBatch& record_batch,
        const org::apache::arrow::flatbuf::Schema& schema,
        const encapsulated_message& encapsulated_message,
        const std::vector<std::optional<std::vector<sparrow::metadata_pair>>>& field_metadata,
        const dictionary_cache& dictionaries
    )
    {
        const size_t num_fields = schema.fields() == nullptr ? 0 : static_cast<size_t>(schema.fields()->size());
        std::vector<sparrow::array> arrays;
        if (num_fields == 0)
        {
            return arrays;
        }
        arrays.reserve(num_fields);
        size_t field_idx = 0;
        size_t buffer_index = 0;
        size_t node_index = 0;
        size_t variadic_counts_idx = 0;
        for (const auto field : *(schema.fields()))
        {
            if (!field)
            {
                throw std::runtime_error("Invalid null field.");
            }
            const std::optional<std::vector<sparrow::metadata_pair>>& metadata = field_metadata[field_idx++];
            const std::string name = field->name() == nullptr ? "" : field->name()->str();
            const bool nullable = field->nullable();

            sparrow::array decoded = field->dictionary() != nullptr
                                        ? deserialize_dictionary_indices(
                                              record_batch,
                                              encapsulated_message.body(),
                                              record_batch.length(),
                                              name,
                                              metadata,
                                              nullable,
                                              buffer_index,
                                              node_index,
                                              *field
                                          )
                                        : array_deserializer::deserialize(
                                              record_batch,
                                              encapsulated_message.body(),
                                              record_batch.length(),
                                              name,
                                              metadata,
                                              nullable,
                                              buffer_index,
                                              node_index,
                                              variadic_counts_idx,
                                              *field
                                          );

            arrays.emplace_back(apply_dictionary_encoding(std::move(decoded), *field, dictionaries));
        }
        return arrays;
    }

    std::vector<sparrow::record_batch> deserialize_stream(std::span<const uint8_t> data)
    {
        const org::apache::arrow::flatbuf::Schema* schema = nullptr;
        std::vector<sparrow::record_batch> record_batches;
        std::vector<std::string> field_names;
        std::vector<bool> fields_nullable;
        std::vector<sparrow::data_type> field_types;
        std::vector<std::optional<std::vector<sparrow::metadata_pair>>> fields_metadata;
        std::unordered_map<int64_t, const org::apache::arrow::flatbuf::Field*> dictionary_fields;
        dictionary_cache dictionaries;

        while (!data.empty())
        {
            // Check for end-of-stream marker
            if (data.size() >= END_OF_STREAM_MARKER_SIZE
                && is_end_of_stream(data.subspan(0, END_OF_STREAM_MARKER_SIZE)))
            {
                break;
            }

            const auto [encapsulated_message, rest] = extract_encapsulated_message(data);
            const org::apache::arrow::flatbuf::Message* message = encapsulated_message.flat_buffer_message();

            if (message == nullptr)
            {
                throw std::invalid_argument("Extracted flatbuffers message is null.");
            }

            switch (message->header_type())
            {
                case org::apache::arrow::flatbuf::MessageHeader::Schema:
                {
                    schema = message->header_as_Schema();
                    const size_t size = schema->fields() == nullptr
                                            ? 0
                                            : static_cast<size_t>(schema->fields()->size());
                    field_names.reserve(size);
                    fields_nullable.reserve(size);
                    fields_metadata.reserve(size);
                    if (schema->fields() == nullptr)
                    {
                        break;
                    }
                    for (const auto field : *(schema->fields()))
                    {
                        if (field != nullptr && field->name() != nullptr)
                        {
                            field_names.emplace_back(field->name()->str());
                        }
                        else
                        {
                            field_names.emplace_back("_unnamed_");
                        }
                        fields_nullable.push_back(field->nullable());
                        const ::flatbuffers::Vector<::flatbuffers::Offset<org::apache::arrow::flatbuf::KeyValue>>*
                            fb_custom_metadata = field->custom_metadata();
                        std::optional<std::vector<sparrow::metadata_pair>>
                            metadata = fb_custom_metadata == nullptr
                                           ? std::nullopt
                                           : std::make_optional(to_sparrow_metadata(*fb_custom_metadata));
                        fields_metadata.push_back(std::move(metadata));

                        if (field != nullptr)
                        {
                            collect_dictionary_fields(*field, dictionary_fields);
                        }
                    }
                }
                break;
                case org::apache::arrow::flatbuf::MessageHeader::RecordBatch:
                {
                    if (schema == nullptr)
                    {
                        throw std::runtime_error("RecordBatch encountered before Schema message.");
                    }
                    const auto* record_batch = message->header_as_RecordBatch();
                    if (record_batch == nullptr)
                    {
                        throw std::runtime_error("RecordBatch message header is null.");
                    }
                    std::vector<sparrow::array> arrays = get_arrays_from_record_batch(
                        *record_batch,
                        *schema,
                        encapsulated_message,
                        fields_metadata,
                        dictionaries
                    );
                    auto names_copy = field_names;
                    sparrow::record_batch sp_record_batch(std::move(names_copy), std::move(arrays));
                    record_batches.emplace_back(std::move(sp_record_batch));
                }
                break;
                case org::apache::arrow::flatbuf::MessageHeader::DictionaryBatch:
                {
                    if (schema == nullptr)
                    {
                        throw std::runtime_error("DictionaryBatch encountered before Schema message.");
                    }

                    const auto* dictionary_batch = message->header_as_DictionaryBatch();
                    if (dictionary_batch == nullptr)
                    {
                        throw std::runtime_error("DictionaryBatch message header is null.");
                    }

                    auto dictionary_data = deserialize_dictionary_batch(
                        *dictionary_batch,
                        encapsulated_message,
                        dictionary_fields
                    );

                    dictionaries.store_dictionary(
                        dictionary_batch->id(),
                        std::move(dictionary_data),
                        dictionary_batch->isDelta()
                    );
                }
                break;
                case org::apache::arrow::flatbuf::MessageHeader::Tensor:
                case org::apache::arrow::flatbuf::MessageHeader::SparseTensor:
                    throw std::runtime_error(
                        "Unsupported message type: Tensor or SparseTensor"
                    );
                default:
                    throw std::runtime_error("Unknown message header type.");
            }
            data = rest;
        }
        return record_batches;
    }
}
