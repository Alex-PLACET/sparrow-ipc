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

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>

#include <sparrow/c_interface.hpp>

#include "sparrow_ipc/config/config.hpp"

namespace sparrow_ipc
{
    struct SPARROW_IPC_API dictionary_metadata
    {
        std::optional<int64_t> id;
        bool is_ordered = false;
    };

    [[nodiscard]] SPARROW_IPC_API int64_t
    compute_fallback_dictionary_id(std::string_view field_name, size_t field_index);

    [[nodiscard]] SPARROW_IPC_API dictionary_metadata
    parse_dictionary_metadata(const ArrowSchema& schema);
}
