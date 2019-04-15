<<<<<<< HEAD
sub_dir :=  $(call source-dir-to-binary-dir, TripleBit/util)
$(shell $(MKDIR) $(sub_dir))

local_src := $(wildcard $(subdirectory)/*.cpp) $(wildcard $(subdirectory)/util/*.cpp)

$(eval $(call make-library,libtriplebit.a,$(local_src)))

$(eval $(call compile-rules))
=======
sub_dir :=  $(call source-dir-to-binary-dir, TripleBit/util)
$(shell $(MKDIR) $(sub_dir))

local_src := $(wildcard $(subdirectory)/*.cpp) $(wildcard $(subdirectory)/util/*.cpp)

$(eval $(call make-library,libtriplebit.a,$(local_src)))

$(eval $(call compile-rules))
>>>>>>> 3b7ac37b57e8fbd77c252b820e37878d3f8d819c
