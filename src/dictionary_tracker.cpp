// Copyright 2024 Man Group Operations Limited
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "sparrow_ipc/dictionary_tracker.hpp"

#include "sparrow_ipc/dictionary_utils.hpp"

#include <sparrow/arrow_interface/arrow_array.hpp>
#include <sparrow/arrow_interface/arrow_schema.hpp>
#include <sparrow/arrow_interface/arrow_array_schema_utils.hpp>

namespace sparrow_ipc
{
    namespace
    {
        /**
         * @brief Extract dictionary ID from ArrowSchema metadata.
         *
         * The dictionary ID can be stored in the schema's metadata with key "ARROW:dictionary:id".
         * If not found, we generate an ID from a stable hash of the field name.
         *
         * @param schema The ArrowSchema to extract ID from
         * @return The dictionary ID
         */
        int64_t extract_dictionary_id(
            const ArrowSchema* schema,
            std::string_view fallback_name,
            size_t fallback_index
        )
        {
            if (schema == nullptr || schema->dictionary == nullptr)
            {
                throw std::invalid_argument("Schema must have a dictionary");
            }

            // Try to extract from metadata first
            const auto metadata = parse_dictionary_metadata(*schema);
            if (metadata.id.has_value())
            {
                return *metadata.id;
            }

            // Fallback: use stable hash of field name
            return compute_fallback_dictionary_id(fallback_name, fallback_index);
        }

        /**
         * @brief Check if ArrowSchema has dictionary encoding.
         *
         * @param schema The ArrowSchema to check
         * @return true if the schema has dictionary encoding, false otherwise
         */
        bool has_dictionary(const ArrowSchema* schema)
        {
            return schema != nullptr && schema->dictionary != nullptr;
        }

    }

    std::vector<dictionary_info> dictionary_tracker::extract_dictionaries_from_batch(
        const sparrow::record_batch& batch
    )
    {
        std::vector<dictionary_info> dictionaries;

        // Scan each column for dictionary encoding
        for (size_t column_idx = 0; column_idx < batch.columns().size(); ++column_idx)
        {
            const auto& column = batch.columns()[column_idx];
            const auto& arrow_proxy = sparrow::detail::array_access::get_arrow_proxy(column);
            const auto& schema = arrow_proxy.schema();

            if (has_dictionary(&schema))
            {
                const auto& names = batch.names();
                const std::string_view fallback_name = column_idx < names.size()
                                                           ? std::string_view(names[column_idx])
                                                           : std::string_view("__dictionary__");
                const int64_t dict_id = extract_dictionary_id(&schema, fallback_name, column_idx);

                // Only include if not already emitted (unless it's a delta update)
                if (!is_emitted(dict_id))
                {
                    // Extract dictionary data
                    // The dictionary is stored in schema.dictionary and the associated ArrowArray
                    
                    // Get the dictionary schema
                    ArrowSchema* dict_schema = schema.dictionary;
                    
                    // We need to get the dictionary ArrowArray from the column's ArrowArray
                    const auto& array = arrow_proxy.array();
                    if (array.dictionary == nullptr)
                    {
                        throw std::runtime_error("ArrowArray must have dictionary data when ArrowSchema has dictionary");
                    }

                    // Create a single-column record batch from dictionary Arrow pointers
                    std::vector<sparrow::array> dict_arrays;
                    dict_arrays.emplace_back(array.dictionary, dict_schema);
                    
                    const std::string dict_name = (dict_schema->name != nullptr && std::string_view(dict_schema->name).size() > 0)
                                                      ? std::string(dict_schema->name)
                                                      : std::string("__dictionary__");
                    std::vector<std::string> dict_names = {dict_name};
                    
                    sparrow::record_batch dict_batch(
                        std::move(dict_names),
                        std::move(dict_arrays)
                    );

                    // Check metadata for ordering
                    const bool is_ordered = parse_dictionary_metadata(schema).is_ordered;

                    dictionary_info info{
                        .id = dict_id,
                        .data = std::move(dict_batch),
                        .is_ordered = is_ordered,
                        .is_delta = false  // Initial emission is never delta
                    };

                    dictionaries.push_back(std::move(info));
                }
            }
        }

        return dictionaries;
    }

    void dictionary_tracker::mark_emitted(int64_t id)
    {
        m_emitted_dict_ids.insert(id);
    }

    bool dictionary_tracker::is_emitted(int64_t id) const
    {
        return m_emitted_dict_ids.contains(id);
    }

    void dictionary_tracker::reset()
    {
        m_emitted_dict_ids.clear();
    }
}
