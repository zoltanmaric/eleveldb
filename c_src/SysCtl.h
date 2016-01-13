// $Id: $

#ifndef ELEVELDB_SYSCTL_H
#define ELEVELDB_SYSCTL_H

/**
 * @file SysCtl.h
 * 
 * Tagged: Tue Sep 29 13:37:14 PDT 2015
 * 
 * @version: $Revision: $, $Date: $
 * 
 * @author /bin/bash: username: command not found
 */
namespace eleveldb {

    class SysCtl {
    public:

        /**
         * Constructor.
         */
        SysCtl();
         
        /**
         * Destructor.
         */
        virtual ~SysCtl();
        
        static uint64_t getVirtualMemoryUsed();

    private:

        static uint64_t getVmLinux();
        static uint64_t getVmApple();
        static uint64_t parseProcLine(char* line);

    }; // End class SysCtl

} // End namespace eleveldb



#endif // End #ifndef ELEVELDB_SYSCTL_H
