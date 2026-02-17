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

#include "sparrow_ipc/dictionary_cache.hpp"

#include <sparrow/arrow_interface/arrow_array_schema_utils.hpp>

namespace sparrow_ipc
{
    void dictionary_cache::store_dictionary(int64_t id, sparrow::record_batch batch, bool is_delta)
    {
        if (batch.nb_columns() != 1)
        {
            throw std::invalid_argument("Dictionary batch must have exactly one column");
        }

        if (is_delta)
        {
            auto it = m_dictionaries.find(id);
            if (it != m_dictionaries.end())
            {
                throw std::runtime_error(
                    "Delta dictionary updates not yet fully implemented - requires array concatenation"
                );
            }
        }

        m_dictionaries.insert_or_assign(id, std::move(batch));
    }

    std::optional<std::reference_wrapper<const sparrow::record_batch>>
    dictionary_cache::get_dictionary(int64_t id) const
    {
        auto it = m_dictionaries.find(id);
        if (it != m_dictionaries.end())
        {
            return std::cref(it->second);
        }
        return std::nullopt;
    }

    bool dictionary_cache::contains(int64_t id) const
    {
        return m_dictionaries.contains(id);
    }

    void dictionary_cache::clear()
    {
        m_dictionaries.clear();
    }

    size_t dictionary_cache::size() const
    {
        return m_dictionaries.size();
    }
}
