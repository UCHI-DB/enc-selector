#include <stdlib.h>
#include "EqualJniScalar.h"

JNIEXPORT jobject JNICALL Java_edu_uchicago_cs_encsel_query_offheap_EqualJniScalar_execute
  (JNIEnv *env, jobject self, jobject input, jint offset, jint size) {

     jlong capacity = size / 8 + ((size %8)?1:0);

     jclass objclass = env->GetObjectClass(self);
     jfieldID entryWidthId = env->GetFieldID(objclass, "entryWidth","I");
     jfieldID targetId = env->GetFieldID(objclass, "target","I");

     jint entryWidth = env->GetIntField(self, entryWidthId);
     jint target = env->GetIntField(self, targetId);

     jbyte* memblock = (jbyte*)malloc(capacity*sizeof(jbyte));
/*
     jclass bbclass = env->GetObjectClass(input);
     jfieldID heapbytesId = env->GetFieldID(bbclass, "hb", "[B");
     jbyteArray heapbytes = (jbyteArray)env->GetObjectField(input, heapbytesId);
     jbyte* bytes = env->GetByteArrayElements(heapbytes, 0);

     jint numbytes = entryWidth / 8 + ((entryWidth %8)?1:0);

     jlong mask = (1L << entryWidth ) - 1;

     for(int i = 0; i < size;i++) {
       // Load data from bytes
       jint srcByte = (entryWidth * i)/8;
       jint srcOffset = (entryWidth*i) %8;

       jint numbits = srcOffset + entryWidth;
       jint numbytes = numbits / 8 + ((numbits%8)?1:0);

       jlong loaded = 0;

       for(int j = 0 ; j < numbytes; j++) {
            jlong byte = *(bytes + srcByte + j);
            loaded |= byte << 8*j;
       }
       loaded = (loaded >> srcOffset) & mask;

       jbyte res = loaded == target;
       jint destByte = i/8;
       jint destOffset = i%8;
       *(memblock+destByte) |= res << destOffset;
     }

*/
     return env->NewDirectByteBuffer(memblock, capacity);
  }