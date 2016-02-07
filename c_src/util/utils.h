//
// Created by Paul A. Place on 2/4/16.
//

#ifndef BASHOUTILS_UTILS_H
#define BASHOUTILS_UTILS_H

// include the other utility header files
#include "buffer.h"
#include "stringUtils.h"
#include "erlangUtils.h"

///////////////////////////////////////
// Some generally useful macros

// returns the count of elements in a C-style array
#define COUNTOF(a) (sizeof(a) / sizeof((a)[0]))

#endif // BASHOUTILS_UTILS_H
