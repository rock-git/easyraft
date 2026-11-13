// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright 2026 easyraft contributors

#pragma once

#include <cstdlib>
#include <optional>
#include <ostream>
#include <string>
#include <utility>

namespace easyraft {

// Project-wide error codes. Storage and protocol failures use the same type so
// callers do not need C++23 std::expected.
enum class Error {
  Ok = 0,
  NotLeader,
  NotInitialized,
  AlreadyInitialized,
  InvalidMembership,
  Storage,
  Rejected,
};

inline const char* to_string(Error error) {
  switch (error) {
    case Error::Ok:
      return "ok";
    case Error::NotLeader:
      return "not leader";
    case Error::NotInitialized:
      return "not initialized";
    case Error::AlreadyInitialized:
      return "already initialized";
    case Error::InvalidMembership:
      return "invalid membership";
    case Error::Storage:
      return "storage";
    case Error::Rejected:
      return "rejected";
  }
  return "unknown";
}

inline std::ostream& operator<<(std::ostream& out, Error error) {
  return out << to_string(error);
}

// Result<T> is a small stand-in for std::expected<T, Error>.
template <typename T>
class Result {
 public:
  static Result success(T value) {
    Result result;
    result.value_ = std::move(value);
    return result;
  }

  static Result failure(Error error) {
    Result result;
    result.error_ = error;
    return result;
  }

  bool ok() const { return value_.has_value(); }
  explicit operator bool() const { return ok(); }
  Error error() const { return value_ ? Error::Ok : error_; }

  T& value() & { return checked(); }
  const T& value() const& { return checked(); }
  T&& value() && { return std::move(checked()); }

  T& operator*() & { return checked(); }
  const T& operator*() const& { return checked(); }
  T* operator->() { return &checked(); }
  const T* operator->() const { return &checked(); }

 private:
  T& checked() {
    if (!value_) {
      std::abort();
    }
    return *value_;
  }
  const T& checked() const {
    if (!value_) {
      std::abort();
    }
    return *value_;
  }

  std::optional<T> value_;
  Error error_{Error::Rejected};
};

template <>
class Result<void> {
 public:
  static Result success() { return Result(true, Error::Ok); }
  static Result failure(Error error) { return Result(false, error); }

  bool ok() const { return ok_; }
  explicit operator bool() const { return ok_; }
  Error error() const { return ok_ ? Error::Ok : error_; }

 private:
  Result(bool ok, Error error) : ok_(ok), error_(error) {}

  bool ok_{false};
  Error error_{Error::Rejected};
};

}  // namespace easyraft
