##*****************************************************************************
#  AUTHOR:
#    Derived from x_ac_json.
#
#  SYNOPSIS:
#    X_AC_PARASTOR()
#
#  DESCRIPTION:
#    Check for parastor burst buffer plugin dependencies.
#    Requires curl and jansson libraries.
#
#  WARNINGS:
#    This macro must be placed after LIBCURL_CHECK_CONFIG and before AC_PROG_LIBTOOL.
##*****************************************************************************

AC_DEFUN([X_AC_PARASTOR], [

  _x_ac_parastor_dirs="/usr /usr/local"
  _x_ac_parastor_libs="lib64 lib"
  x_ac_cv_jansson_dir=""
  x_ac_have_parastor="no"

  AC_ARG_WITH(
    [parastor],
    AS_HELP_STRING(--with-parastor@<:@=yes|no|PATH@:>@,Build burst_buffer/parastor plugin (requires curl and jansson)),
    [AS_IF([test "x$with_parastor" != xno && test "x$with_parastor" != xyes],
	   [_x_ac_parastor_dirs="$with_parastor"])])

  # Only check for dependencies if --with-parastor=yes or --with-parastor is explicitly specified
  if [test "x$with_parastor" = xno] || [test -z "$with_parastor"]; then
    AC_MSG_NOTICE([parastor plugin support is disabled (use --with-parastor to enable)])
  else
    # First check if curl is available (should already be checked by LIBCURL_CHECK_CONFIG)
    if test x"$libcurl_cv_lib_curl_usable" != xyes; then
      if [test -z "$with_parastor"] ; then
        AC_MSG_WARN([curl not found, parastor plugin will not be built])
      else
        AC_MSG_ERROR([--with-parastor requires curl, but curl was not found.])
      fi
    else
      # Try to use pkg-config first
      if test -n "$PKG_CONFIG"; then
        PKG_CHECK_MODULES([PARASTOR], [jansson], [
          x_ac_have_parastor="yes"
          x_ac_cv_jansson_dir="pkg-config"
        ], [
          x_ac_have_parastor="no"
        ])
      fi

      # If pkg-config failed, try manual search
      if test "x$x_ac_have_parastor" != "xyes"; then
        AC_CACHE_CHECK(
          [for jansson installation],
          [x_ac_cv_jansson_dir],
          [
            _x_ac_jansson_libdir=""
            for d in $_x_ac_parastor_dirs; do
              test -d "$d" || continue
              test -d "$d/include" || continue
              test -f "$d/include/jansson.h" || continue
              for bit in $_x_ac_parastor_libs; do
                test -d "$d/$bit" || continue
                _x_ac_jansson_libs_save="$LIBS"
                LIBS="-L$d/$bit -ljansson $LIBS"
                AC_LINK_IFELSE(
                  [AC_LANG_CALL([], json_loads)],
                  [AS_VAR_SET(x_ac_cv_jansson_dir, $d)
                   _x_ac_jansson_libdir="$bit"])
                LIBS="$_x_ac_jansson_libs_save"
                test -n "$x_ac_cv_jansson_dir" && break
              done
              test -n "$x_ac_cv_jansson_dir" && break
            done
          ])

        if test -n "$x_ac_cv_jansson_dir"; then
          x_ac_have_parastor="yes"
        fi
      fi

      if test "x$x_ac_have_parastor" != "xyes"; then
        if [test -z "$with_parastor"] ; then
          AC_MSG_WARN([unable to locate jansson library, parastor plugin will not be built])
        else
          AC_MSG_ERROR([unable to locate jansson library. Install jansson-devel or pass --with-parastor=PATH.])
        fi
        PARASTOR_CPPFLAGS=""
        PARASTOR_LDFLAGS=""
      else
        AC_DEFINE([HAVE_PARASTOR], [1], [Define if you are compiling with parastor burst buffer plugin.])
        if test "x$x_ac_cv_jansson_dir" = "xpkg-config"; then
          # Use pkg-config flags
          PARASTOR_CPPFLAGS="$PARASTOR_CFLAGS"
          PARASTOR_LDFLAGS="$PARASTOR_LIBS"
        else
          # Use manual search flags
          PARASTOR_CPPFLAGS="-I$x_ac_cv_jansson_dir/include"
          PARASTOR_LDFLAGS="-L$x_ac_cv_jansson_dir/$_x_ac_jansson_libdir -ljansson"
        fi
      fi
    fi
  fi

  AC_SUBST(PARASTOR_CPPFLAGS)
  AC_SUBST(PARASTOR_LDFLAGS)
  AM_CONDITIONAL(WITH_PARASTOR, test "x$x_ac_have_parastor" = "xyes" && test x"$libcurl_cv_lib_curl_usable" = xyes)
])

