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
#include <set>
#include <vector>

#include <sparrow/record_batch.hpp>

#include "sparrow_ipc/config/config.hpp"

namespace sparrow_ipc
{
    /**
     * @brief Information about a dictionary used for encoding.
     */
    struct dictionary_info
    {
        int64_t id;                      ///< Dictionary identifier
        sparrow::record_batch data;      ///< Dictionary values as a single-column record batch
        bool is_ordered;                 ///< Whether dictionary values are ordered
        bool is_delta;                   ///< Whether this is a delta update
    };

    /**
     * @brief Tracks dictionaries during serialization.
     *
     * This class is responsible for discovering dictionary-encoded fields in record batches,
     * extracting their dictionary data, and managing which dictionaries have been emitted
     * to the output stream.
     *
     * Dictionaries must be emitted before any RecordBatch that references them. This class
     * ensures proper ordering by tracking emitted dictionary IDs and providing methods to
     * determine which dictionaries need to be sent before each record batch.
     */
    class SPARROW_IPC_API dictionary_tracker
    {
    public:
        /**
         * @brief Extract dictionaries from a record batch.
         *
         * Scans all columns in the record batch for dictionary-encoded fields.
         * Returns a vector of dictionaries that need to be emitted before this
         * record batch can be serialized.
         *
         * @param batch The record batch to scan for dictionaries
         * @return Vector of dictionary_info for dictionaries that haven't been emitted yet
         */
        std::vector<dictionary_info> extract_dictionaries_from_batch(const sparrow::record_batch& batch);

        /**
         * @brief Mark a dictionary as emitted.
         *
         * After a dictionary has been written to the stream, call this method to
         * record that it has been emitted. This prevents re-emission of the same
         * dictionary for subsequent record batches (unless it's a delta update).
         *
         * @param id The dictionary ID that was emitted
         * @param is_delta Whether this was a delta update
         */
        void mark_emitted(int64_t id, bool is_delta);

        /**
         * @brief Check if a dictionary has been emitted.
         *
         * @param id The dictionary ID to check
         * @return true if the dictionary has been emitted, false otherwise
         */
        bool is_emitted(int64_t id) const;

        /**
         * @brief Reset tracking state.
         *
         * Clears all tracking information. Useful when starting a new stream.
         */
        void reset();

    private:
        std::set<int64_t> m_emitted_dict_ids;  ///< IDs of dictionaries already emitted
    };
}
