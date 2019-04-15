<<<<<<< HEAD
local_src := $(wildcard $(subdirectory)/*.cpp)

$(eval $(call make-program,triplebitQuery,libtriplebit.a,$(local_src)))

$(eval $(call compile-rules))
=======
local_src := $(wildcard $(subdirectory)/*.cpp)

$(eval $(call make-program,triplebitQuery,libtriplebit.a,$(local_src)))

$(eval $(call compile-rules))
>>>>>>> 3b7ac37b57e8fbd77c252b820e37878d3f8d819c
