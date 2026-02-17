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

#include "sparrow_ipc/dictionary_utils.hpp"

#include <string>

#include <sparrow/utils/metadata.hpp>

namespace sparrow_ipc
{
    int64_t compute_fallback_dictionary_id(std::string_view field_name, size_t field_index)
    {
        uint64_t hash = 1469598103934665603ULL;
        for (const unsigned char c : field_name)
        {
            hash ^= static_cast<uint64_t>(c);
            hash *= 1099511628211ULL;
        }

        hash ^= static_cast<uint64_t>(field_index + 1);
        hash *= 1099511628211ULL;

        return static_cast<int64_t>(hash);
    }

    dictionary_metadata parse_dictionary_metadata(const ArrowSchema& schema)
    {
        dictionary_metadata metadata;

        if (schema.metadata == nullptr)
        {
            return metadata;
        }

        const auto metadata_view = sparrow::key_value_view(schema.metadata);
        for (const auto& [key, value] : metadata_view)
        {
            if (key == "ARROW:dictionary:id")
            {
                metadata.id = std::stoll(std::string(value));
            }
            else if (key == "ARROW:dictionary:ordered")
            {
                metadata.is_ordered = (value == "true" || value == "1");
            }
        }

        return metadata;
    }
}
