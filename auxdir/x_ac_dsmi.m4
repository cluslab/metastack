##*****************************************************************************
#  AUTHOR:
#    Advanced Micro Devices
#
#  SYNOPSIS:
#    X_AC_DSMI
#
#  DESCRIPTION:
#    Determine if HUAWEI's DSMI API library exists
##*****************************************************************************

AC_DEFUN([X_AC_DSMI],
[

  # /usr/local/Ascend is the current default location.
  # We will use a for loop to check for both.
  # Unless _x_ac_dsmi_dirs is overwritten with --with-dsmi
  _x_ac_dsmi_dirs="/usr/local/Ascend"

  AC_ARG_WITH(
    [dsmi],
    AS_HELP_STRING(--with-dsmi=PATH, Specify path to dsmi installation),
    [AS_IF([test "x$with_dsmi" != xno && test "x$with_dsmi" != xyes],
           [_x_ac_dsmi_dirs="$with_dsmi"])])

  if [test "x$with_dsmi" = xno]; then
     AC_MSG_WARN([support for dsmi disabled])
  else
    AC_MSG_CHECKING([whether DSMI in installed in this system])
    # Check for DSMI header and library in the default location
    # or in the location specified during configure
    AC_MSG_RESULT([])

    cppflags_save="$CPPFLAGS"
    ldflags_save="$LDFLAGS"
    CPPFLAGS="-I$_x_ac_dsmi_dir/driver/kernel/inc/driver $CPPFLAGS"
    LDFLAGS="-L$_x_ac_dsmi_dir/driver/lib64/driver $LDFLAGS"
    AC_CHECK_HEADER([dsmi_common_interface.h], [ac_dsmi_h=yes], [ac_dsmi_h=no])
    CPPFLAGS="$cppflags_save"
    LDFLAGS="$ldflags_save"
    if test "$ac_dsmi_h" = "yes"; then
        DSMI_LDFLAGS="-L$_x_ac_dsmi_dir/driver/lib64/driver"
        DSMI_LIBS="-ldrvdsmi_host"
        DSMI_CPPFLAGS="-I$_x_ac_dsmi_dir/driver/kernel/inc/driver"
        ac_dsmi="yes"
        AC_DEFINE(HAVE_DSMI, 1, [Define to 1 if DSMI library found])
    else
        AC_MSG_WARN([unable to locate libdrvdsmi_host.so and/or dsmi_common_interface.h])
    fi
    
    AC_SUBST(DSMI_LIBS)
    AC_SUBST(DSMI_CPPFLAGS)
    AC_SUBST(DSMI_LDFLAGS)
  fi
  AM_CONDITIONAL(BUILD_DSMI, test "$ac_dsmi" = "yes")
])
