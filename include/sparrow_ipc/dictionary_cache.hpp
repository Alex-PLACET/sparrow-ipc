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

#pragma once

#include <cstdint>
#include <map>
#include <optional>

#include <sparrow/record_batch.hpp>

#include "sparrow_ipc/config/config.hpp"

namespace sparrow_ipc
{
    /**
     * @brief Caches dictionaries during deserialization.
     *
     * This class stores dictionaries received from DictionaryBatch messages and
     * provides them when reconstructing dictionary-encoded arrays. It handles
     * both delta updates (appending to existing dictionaries) and replacement
     * (overwriting existing dictionaries).
     *
     * Dictionaries are stored as single-column record batches and are referenced
     * by their integer ID. Multiple fields can share the same dictionary by
     * referencing the same ID.
     */
    class SPARROW_IPC_API dictionary_cache
    {
    public:
        /**
         * @brief Store or update a dictionary.
         *
         * Stores or updates a dictionary identified by the given ID. Currently,
         * the is_delta parameter is reserved for future use and is not supported.
         * The dictionary is always replaced (or inserted if it doesn't exist).
         *
         * @param id The dictionary ID
         * @param batch The dictionary data as a single-column record batch
         * @param is_delta Reserved for future use (delta updates not yet supported)
         * @throws std::invalid_argument if batch doesn't have exactly one column
         */
        void store_dictionary(int64_t id, sparrow::record_batch batch, bool is_delta);

        /**
         * @brief Retrieve a cached dictionary.
         *
         * @param id The dictionary ID to retrieve
         * @return An optional containing the dictionary if found, std::nullopt otherwise
         */
        std::optional<std::reference_wrapper<const sparrow::record_batch>> get_dictionary(int64_t id) const;

        /**
         * @brief Check if a dictionary is cached.
         *
         * @param id The dictionary ID to check
         * @return true if the dictionary exists in the cache, false otherwise
         */
        bool contains(int64_t id) const;

        /**
         * @brief Clear all cached dictionaries.
         */
        void clear();

        /**
         * @brief Get the number of cached dictionaries.
         *
         * @return The number of dictionaries in the cache
         */
        size_t size() const;

    private:
        std::map<int64_t, sparrow::record_batch> m_dictionaries;
    };
}
