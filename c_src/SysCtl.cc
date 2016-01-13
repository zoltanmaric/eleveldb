#include <inttypes.h>

#ifdef __APPLE__
#include<mach/mach.h>
#endif

#include "SysCtl.h"

#include "exceptionutils.h"

using namespace std;

using namespace eleveldb;

/**.......................................................................
 * Constructor.
 */
SysCtl::SysCtl() {}

/**.......................................................................
 * Destructor.
 */
SysCtl::~SysCtl() {}

uint64_t SysCtl::getVirtualMemoryUsed()
{
#ifdef __APPLE__
  return getVmApple();
#endif

#ifdef __linux
  return getVmLinux();
#endif
}

/**.......................................................................
 * Get virtual memory under Mac OS
 */
uint64_t SysCtl::getVmApple()
{
#ifndef __APPLE__
  ThrowRuntimeError("Called apple fn, but this is not a Mac OS");
  return 0;
#else
  struct task_basic_info t_info;
  mach_msg_type_number_t t_info_count = TASK_BASIC_INFO_COUNT;

  if(KERN_SUCCESS != task_info(mach_task_self(),
			       TASK_BASIC_INFO, (task_info_t)&t_info,
			       &t_info_count)) {
    return -1;
  }

  return t_info.virtual_size;
#endif
}

/**.......................................................................
 * Parse a line out of the proc filesystem
 */
uint64_t SysCtl::parseProcLine(char* line)
{
  int i = strlen(line);
  while (*line < '0' || *line > '9') line++;
  line[i-3] = '\0';
  return strtol(line, 0, 10);
}

/**.......................................................................
 * Get virtual memory under linux
 */
uint64_t SysCtl::getVmLinux()
{
#ifndef __linux
  ThrowRuntimeError("Called linux fn, but this is not a Linux OS");
  return 0;
#else

  FILE* file = fopen("/proc/self/status", "r");
  uint64_t result = 0;
  char line[128];
  
  while (fgets(line, 128, file) != NULL){
    if (strncmp(line, "VmSize:", 7) == 0){
      result = parseProcLine(line);
      break;
    }
  }

  fclose(file);
  return result * 1000;
#endif
}
