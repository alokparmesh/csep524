#define CL_USE_DEPRECATED_OPENCL_1_2_APIS
#ifndef _opencl_interface_h
#define _opencl_interface_h
//#include <OpenCL/opencl.h>
#include <CL/cl.h>

void opencl_end();
void opencl_start();
cl_program opencl_compile_program(char *src);
cl_device_id opencl_get_device();
cl_command_queue opencl_get_queue();
cl_context opencl_get_context();
const char *open_cl_error_string(cl_int error);

#define clCheck(x) \
    if (x != CL_SUCCESS) {\
        printf("%s - Failed error %d:%s\n", #x, x, open_cl_error_string(x)); \
        exit(-__LINE__);\
    }
#define clCheckErr(x, str) \
    if (x != CL_SUCCESS) {\
        printf("%s - Failed error: %d:%s\n", str, x, open_cl_error_string(x));\
        exit(-__LINE__);\
    }

#define OPENCL_CODE(...)    #__VA_ARGS__
#endif
