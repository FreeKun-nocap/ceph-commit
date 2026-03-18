#ifndef CEPH_ASSERT_H
#define CEPH_ASSERT_H

#include <cstdlib>
#include <string>

#ifndef __STRING
# define __STRING(x) #x
#endif

#if defined(__linux__)
#include <features.h>

#elif defined(__FreeBSD__)
#include <sys/cdefs.h>
#define	__GNUC_PREREQ(minor, major)	__GNUC_PREREQ__(minor, major)
#elif defined(__sun) || defined(_AIX)
#include "include/compat.h"
#include <assert.h>
#endif

#ifdef __CEPH__
# include "acconfig.h"
#endif

#include "include/common_fwd.h"

namespace ceph {

struct BackTrace;

/*
 * Select a function-name variable based on compiler tests, and any compiler
 * specific overrides.
 */
/**
预处理器条件选择块，用于在不同的编译环境下选择合适的函数名变量
HAVE_PRETTY_FUNC 和 HAVE_FUNC 宏是通过项目的构建系统在编译前自动检测生成的，通常由 Autoconf 脚本完成
在断言失败时，能够显示当前正在执行的函数名称，以便于调试
*/
#if defined(HAVE_PRETTY_FUNC)
# define __CEPH_ASSERT_FUNCTION __PRETTY_FUNCTION__
#elif defined(HAVE_FUNC)
# define __CEPH_ASSERT_FUNCTION __func__
#else
# define __CEPH_ASSERT_FUNCTION ((__const char *) 0)
#endif

extern void register_assert_context(CephContext *cct);

struct assert_data {
  const char *assertion;
  const char *file;
  const int line;
  const char *function;
};

extern void __ceph_assert_fail(const char *assertion, const char *file, int line, const char *function);
extern void __ceph_assert_fail(const assert_data &ctx);
template <const assert_data* AssertCtxV>
[[gnu::noinline, gnu::cold]] static void __ceph_assert_fail()  {
  __ceph_assert_fail(*AssertCtxV);
}

extern void __ceph_assertf_fail(const char *assertion, const char *file, int line, const char *function, const char* msg, ...);
extern void __ceph_assert_warn(const char *assertion, const char *file, int line, const char *function);

[[noreturn]] void __ceph_abort(const char *file, int line, const char *func,
                               const std::string& msg);

[[noreturn]] void __ceph_abortf(const char *file, int line, const char *func,
                                const char* msg, ...);
/**
C++ 中的类型转换运算符，用于执行静态类型转换
*/
#define _CEPH_ASSERT_VOID_CAST static_cast<void>

#define assert_warn(expr)							\
  ((expr)								\
   ? _CEPH_ASSERT_VOID_CAST (0)					\
   : ::ceph::__ceph_assert_warn (__STRING(expr), __FILE__, __LINE__, __CEPH_ASSERT_FUNCTION))

}

using namespace ceph;


/*
 * ceph_abort aborts the program with a nice backtrace.
 *
 * Currently, it's the same as assert(0), but we may one day make assert a
 * debug-only thing, like it is in many projects.
 */
#define ceph_abort(msg, ...)                                            \
  ::ceph::__ceph_abort( __FILE__, __LINE__, __CEPH_ASSERT_FUNCTION, "abort() called")

#define ceph_abort_msg(msg)                                             \
  ::ceph::__ceph_abort( __FILE__, __LINE__, __CEPH_ASSERT_FUNCTION, msg) 

#define ceph_abort_msgf(...)                                             \
  ::ceph::__ceph_abortf( __FILE__, __LINE__, __CEPH_ASSERT_FUNCTION, __VA_ARGS__)

/**
__SANITIZE_ADDRESS__ 是一个预处理器宏，当使用AddressSanitizer（ASan）编译代码时自动定义
AddressSanitizer是Google开发的一个快速内存错误检测工具，主要用于C/C++程序。它能检测多种内存
ASan在编译时插入额外的检查代码，需要清晰的代码路径
ASan版本避免使用静态变量缓存，因为这些静态变量可能会干扰ASan的内存检查，简化断言有助于获得更清晰的调用栈和错误报告
ASan版本：牺牲性能换取更精确的内存检查
普通版本：优化性能，减少代码体积
*/
/**
若 expr 为 true，则不执行任何操作
若 expr 为 false，则调用 __ceph_assert_fail 函数触发断言失败
*/
#ifdef __SANITIZE_ADDRESS__
#define ceph_assert(expr)                           \
  do {                                              \
    ((expr))                                        \
    ? _CEPH_ASSERT_VOID_CAST (0)                    \
      : ::ceph::__ceph_assert_fail(__STRING(expr), __FILE__, __LINE__, __CEPH_ASSERT_FUNCTION); \
  } while (false)
#else
#define ceph_assert(expr)							\
  do { \
    static const auto func_name = __CEPH_ASSERT_FUNCTION; \
    [] (const bool eval) { \
    static const ceph::assert_data assert_data_ctx = \
    {__STRING(expr), __FILE__, __LINE__, func_name}; \
    ((eval) \
    ? _CEPH_ASSERT_VOID_CAST (0) \
    : ::ceph::__ceph_assert_fail<&assert_data_ctx>()); }((bool)(expr)); } while(false)
#endif

// this variant will *never* get compiled out to NDEBUG in the future.
// (ceph_assert currently doesn't either, but in the future it might.)
#ifdef __SANITIZE_ADDRESS__
#define ceph_assert_always(expr)                    \
  do {                                              \
    ((expr))                                        \
    ? _CEPH_ASSERT_VOID_CAST (0)                    \
      : ::ceph::__ceph_assert_fail(__STRING(expr), __FILE__, __LINE__, __CEPH_ASSERT_FUNCTION); \
  } while(false)
#else
#define ceph_assert_always(expr)							\
  do { \
    static const auto func_name = __CEPH_ASSERT_FUNCTION; \
    [] (const bool eval) { \
    static const ceph::assert_data assert_data_ctx = \
    {__STRING(expr), __FILE__, __LINE__, func_name}; \
    ((eval) \
    ? _CEPH_ASSERT_VOID_CAST (0) \
    : ::ceph::__ceph_assert_fail<&assert_data_ctx>()); }((bool)(expr)); } while(false)
#endif

// Named by analogy with printf.  Along with an expression, takes a format
// string and parameters which are printed if the assertion fails.
#define assertf(expr, ...)                  \
  ((expr)								\
   ? _CEPH_ASSERT_VOID_CAST (0)					\
   : ::ceph::__ceph_assertf_fail (__STRING(expr), __FILE__, __LINE__, __CEPH_ASSERT_FUNCTION, __VA_ARGS__))
#define ceph_assertf(expr, ...)                  \
  ((expr)								\
   ? _CEPH_ASSERT_VOID_CAST (0)					\
   : ::ceph::__ceph_assertf_fail (__STRING(expr), __FILE__, __LINE__, __CEPH_ASSERT_FUNCTION, __VA_ARGS__))

// this variant will *never* get compiled out to NDEBUG in the future.
// (ceph_assertf currently doesn't either, but in the future it might.)
#define ceph_assertf_always(expr, ...)                  \
  ((expr)								\
   ? _CEPH_ASSERT_VOID_CAST (0)					\
   : ::ceph::__ceph_assertf_fail (__STRING(expr), __FILE__, __LINE__, __CEPH_ASSERT_FUNCTION, __VA_ARGS__))

#define consteval_assert(expr, msg)	\
  do {					\
    if (!(expr)) {			\
      throw (msg);			\
    }					\
  } while(false)
#endif
