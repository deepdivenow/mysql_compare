//
// Created by dro on 03.05.18.
//

#include <stdarg.h>  // For va_start, etc.
#include <cstring>
#include <memory>    // For std::unique_ptr

std::string string_format(const std::string fmt_str, ...);
