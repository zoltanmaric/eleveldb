# -------------------------------------------------------------------
#
# Copyright (c) 2016 Basho Technologies, Inc.
#
# This file is provided to you under the Apache License,
# Version 2.0 (the "License"); you may not use this file
# except in compliance with the License.  You may obtain
# a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# -------------------------------------------------------------------

#
# shortcut around using autoconf for snappy
#
# Aside from how heavyweight autoconf is in general, they've added stuff
# that requires even more than the basic ac installation for libtool support,
# which we don't need at all. Since the test's are pretty simple anyway, it's
# easier to just do it in a script and be done with it.
#
# params:
#   config-h-output-file
#   stubs-h-output-file
#   snappy-source-dir
#   capability-test-source-dir
#   capability-test-output-dir
#   C++-compiler (may contain exe and args, no spaces in path)
#
# todo: get the HAVE_ macros from the sources
#

tools_dir="$(dirname "$0")"
conf_file="$1"
stub_file="$2"
snappydir="$3"
capsrcdir="$4"
capoutdir="$5"
cccompile="$6"

if [ $# -ne 6 ]
then
    echo "$(basename "$0"): error: invalid invocation, refer to script" >&2
    exit 1
fi

for d in "$(dirname "$conf_file")" "$(dirname "$stub_file")" "$capoutdir"
do
    if [ ! -d "$d" ]
    then
        /bin/mkdir -p "$d"
    fi
done

comp_test="$capoutdir/comp_test"
sed_filt="$capoutdir/$(basename "$0").sed"

version="$(cd "$snappydir" && git describe --tags 2>/dev/null | cut -d- -f1)"
vsn_major="$(echo "$version" | cut -d. -f1)"
vsn_minor="$(echo "$version" | cut -d. -f2)"
vsn_patch="$(echo "$version" | cut -d. -f3)"

printf \
    's/@SNAPPY_MAJOR@/%s/g\ns/@SNAPPY_MINOR@/%s/g\ns/@SNAPPY_PATCHLEVEL@/%s/g\n' \
    "${vsn_major:-0}" "${vsn_minor:-0}" "${vsn_patch:-0}" > "$sed_filt"

printf '/*\n * This is a generated file - DO NOT EDIT!\n */\n\n' > "$conf_file"
# for completeness
printf '#define HAVE_%s 1\n\n' "$(basename "$conf_file" \
    | tr '[a-z.]' '[A-Z_]')" >> "$conf_file"

# valid C or C++
incl_code='#include <%s.h>\nint main(void)\n{\n return 0;\n}\n'
retn_code='int main(void)\n{\n return %s;\n}\n'

# HAVE_BUILTIN_CTZ

macro='HAVE_BUILTIN_CTZ'
printf "$retn_code" '(__builtin_ctzll(0x100000000LL) == 32) ? 1 : 0' > "$comp_test.cc"
if $cccompile -c -o "$comp_test.o" "$comp_test.cc" 1>/dev/null 2>&1
then
    printf '#define %s 1\n' "$macro" >> "$conf_file"
else
    printf '#undef  %s\n' "$macro" >> "$conf_file"
fi

# HAVE_BUILTIN_EXPECT

macro='HAVE_BUILTIN_EXPECT'
printf "$retn_code" '__builtin_expect(1, 1) ? 1 : 0' > "$comp_test.cc"
if $cccompile -c -o "$comp_test.o" "$comp_test.cc" 1>/dev/null 2>&1
then
    printf '#define %s 1\n' "$macro" >> "$conf_file"
else
    printf '#undef  %s\n' "$macro" >> "$conf_file"
fi

# HAVE_FUNC_MMAP

macro='HAVE_FUNC_MMAP'
acvar='ac_cv_func_mmap'
printf \
    'extern "C" char mmap(void);\nint main(void)\n{\n return mmap();\n}\n' \
    > "$comp_test.cc"
if $cccompile -o "$comp_test" "$comp_test.cc" 1>/dev/null 2>&1
then
    printf '#define %s 1\n' "$macro" >> "$conf_file"
    printf 's/@%s@/1/g\n' "$acvar" >> "$sed_filt"
else
    printf '#undef  %s\n' "$macro" >> "$conf_file"
    printf 's/@%s@/0/g\n' "$acvar" >> "$sed_filt"
fi

printf '\n' >> "$conf_file"

# HAVE_GFLAGS
# HAVE_GTEST
#
# these are for tests, we don't care
#
printf '#undef  %s\n' 'HAVE_GFLAGS' >> "$conf_file"
printf 's/@%s@/0/g\n' 'ac_cv_have_gflags' >> "$sed_filt"
printf '#undef  %s\n' 'HAVE_GTEST' >> "$conf_file"
printf 's/@%s@/0/g\n' 'ac_cv_have_gtest' >> "$sed_filt"

# HAVE_BYTESWAP_H
# HAVE_CONFIG_H
# HAVE_DLFCN_H
# HAVE_INTTYPES_H
# HAVE_MEMORY_H
# HAVE_STDDEF_H
# HAVE_STDINT_H
# HAVE_STDLIB_H
# HAVE_STRINGS_H
# HAVE_STRING_H
# HAVE_SYS_BYTEORDER_H
# HAVE_SYS_BYTESWAP_H
# HAVE_SYS_ENDIAN_H
# HAVE_SYS_MMAN_H
# HAVE_SYS_RESOURCE_H
# HAVE_SYS_STAT_H
# HAVE_SYS_TIME_H
# HAVE_SYS_TYPES_H
# HAVE_UNISTD_H
# HAVE_WINDOWS_H

printf '\n' >> "$conf_file"

for n in \
    byteswap \
    dlfcn \
    inttypes \
    memory \
    stddef \
    stdint \
    stdlib \
    strings \
    string \
    sys/byteorder \
    sys/byteswap \
    sys/endian \
    sys/mman \
    sys/resource \
    sys/stat \
    sys/time \
    sys/types \
    sys/uio \
    unistd \
    windows
do
    macro="$(printf 'HAVE_%s_H' "$n" | tr '[a-z/]' '[A-Z_]')"
    acvar="$(printf 'ac_cv_have_%s_h' "$n" | tr '[A-Z/]' '[a-z_]')"
    printf "$incl_code" "$n" > "$comp_test.cc"
    if $cccompile -c -o "$comp_test.o" "$comp_test.cc" 1>/dev/null 2>&1
    then
        printf '#define %s 1\n' "$macro" >> "$conf_file"
        printf 's/@%s@/1/g\n' "$acvar" >> "$sed_filt"
    else
        printf '#undef  %s\n' "$macro" >> "$conf_file"
        printf 's/@%s@/0/g\n' "$acvar" >> "$sed_filt"
    fi
done

# HAVE_LIBFASTLZ
# HAVE_LIBLZF
# HAVE_LIBLZO2
# HAVE_LIBQUICKLZ
# HAVE_LIBZ

printf '\n' >> "$conf_file"

for n in fastlz lzf lzo2 quicklz z
do
    macro="$(printf 'HAVE_LIB%s' "$n" | tr '[a-z/]' '[A-Z_]')"
    acvar="$(printf 'ac_cv_have_lib%s' "$n" | tr '[A-Z/]' '[a-z_]')"
    if $cccompile -o "$comp_test" "-l$n" "$capsrcdir/main.cc" 1>/dev/null 2>&1
    then
        printf '#define %s 1\n' "$macro" >> "$conf_file"
        printf 's/@%s@/1/g\n' "$acvar" >> "$sed_filt"
    else
        printf '#undef  %s\n' "$macro" >> "$conf_file"
        printf 's/@%s@/0/g\n' "$acvar" >> "$sed_filt"
    fi
done >> "$conf_file"

#
# a couple of straglers that may matter ...
# these appear in the autoconf-generated config.h file, some only in older
# versions, but it's not clear how many may be used for the compilation
# we're doing, so just throw them in - they're easy
#
for n in is_stdc endian
do
    if [ ! -f "$capoutdir/cc_$n" ]
    then
        if ! $cccompile -o "$capoutdir/cc_$n" "$capsrcdir/$n.cc"
        then
            # should never be an error compiling these
            # error message should have been written by the compiler
            exit 1
        fi
    fi
done

printf '\n' >> "$conf_file"

printf '#define %s "%s"\n' 'LT_OBJDIR' '.libs/' >> "$conf_file"
printf '#define %s "%s"\n' 'PACKAGE' 'snappy' >> "$conf_file"
printf '#define %s "%s"\n' 'PACKAGE_BUGREPORT' 'https://github.com/google/snappy/issues' >> "$conf_file"
printf '#define %s "%s"\n' 'PACKAGE_NAME' 'snappy' >> "$conf_file"
printf '#define %s "%s"\n' 'PACKAGE_STRING' "snappy $version" >> "$conf_file"
printf '#define %s "%s"\n' 'PACKAGE_TARNAME' 'snappy' >> "$conf_file"
printf '#define %s "%s"\n' 'PACKAGE_URL' 'http://google.github.io/snappy/' >> "$conf_file"
printf '#define %s "%s"\n' 'PACKAGE_VERSION' "$version" >> "$conf_file"
printf '#define %s "%s"\n' 'VERSION' "$version" >> "$conf_file"

printf '\n' >> "$conf_file"

if "$capoutdir/cc_is_stdc"
then
    printf '#define %s 1\n' 'STDC_HEADERS' >> "$conf_file"
else
    printf '#undef  %s\n' 'STDC_HEADERS' >> "$conf_file"
fi
if "$capoutdir/cc_endian"
then
    printf '#undef  %s\n' 'WORDS_BIGENDIAN' >> "$conf_file"
else
    printf '#define %s 1\n' 'WORDS_BIGENDIAN' >> "$conf_file"
fi

sed -f "$sed_filt" < "$snappydir/$(basename "$stub_file").in" > "$stub_file"
