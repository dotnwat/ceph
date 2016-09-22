======================
SDK for object classes
======================

The aim of this project is to create a public interface, which can be used to build object classes outside the `Ceph` tree. Currently, existing object classes are built in-tree. This restricts developers to build the entire Ceph tree to be able to use the object classes. However, with the help of this public interface, object classes can be built and distributed separately as packages.

Step 1: Create basic objclass-public.h
--------------------------------------

All the existing object classes use ``objclass.h`` as an interface to communicate with `Ceph`. ``objclass.h`` has dependencies on the src tree. What we want is an interface without any dependencies on the `Ceph` src tree, so that we do not expose the internal implementations to object classes being built outside the src tree. We have come up with ``objclass-public.h`` for this purpose. The idea is to provide existing users with same functionality by means of an expanded object class API. 

- A sample object class ``cls_ext_test`` has been created in the 
  ``examples/objclass``, which makes use of the public interface. This class 
  can be loaded in `Ceph` by copying it to ``/build/lib`` and restarting `Ceph`.
  This is done to verify the correctness of ``objclass-public.h``.
- The ``cls_hello`` class has been moved outside the ``cls`` directory to demonstrate
  out of tree development of object classes.
- These two classes serve as examples for anybody who would like to build object classes outside the `Ceph` src tree.
- The other classes present in the `cls` directory have not been moved outside the tree. These classes are most likely going to remain in their current location because of the kind of purposes they serve.

Step 2: Install objclass-public.h in user space
-----------------------------------------------

Once you run `make install`, you can find the public header under ``/usr/local/include/rados``. ::

        ls /usr/local/include/rados

Step 3: Eliminate in-tree dependencies
--------------------------------------

The existing ``objclass.h`` has multiple dependencies on the src tree. We want to avoid these for the public interface. We have made few additions to the existing API and modified some bits to achieve our goal.

- One example is the addition of ``cls_get_request_origin2`` and ``cls_serialize`` to replace the functionality of ``cls_get_request_origin``. This has been done to avoid the include of ``/msg/msg_types.h``.
- Existing ``cls_get_request_origin`` is deprecated and any cls library that needs to be used as a self contained package must not use this. Using the function will require includes from within the `Ceph` src tree.
- In order to avoid the include of ``common/ceph_time.h``, ``cls_cxx_stat2`` has been deprecated to be used only by object classes within the tree.
- Similarly, the include of ``include/hobject.h`` can be dealt with, but has not been done for now. It is a part of the ``PGLSFilter`` class, which has not been made a part of ``objclass-public.h``.

Step 4: Create disjoint object class headers
--------------------------------------------

            +----------------------------------+
            |     Ceph Object Class Interface  |
            +------------+---------------------+
            | objclass.h |   objclass-public.h |
            +------------+---------------------+

The goal is to have a unified object class interface for all object classes but we are not there yet. The PGLSFilter class has not been moved to ``objclass-public.h``.

- Two disjoint headers ``objclass.h`` and ``objclass-public.h`` have been created.
- ``objclass.h`` only contains the ``PGLSFilter`` class and its associated 
  functions. ``cls_cxx_snap_revert`` is also present in ``objclass.h`` but since none of
  the existing classes make use of it, this should not pose any problem.
- All the object classes in ``examples/objclass`` and ``src/cls`` have been modified 
  to use ``objclass-public.h`` without the filter functionality.
- ``cls_hello`` and ``cls_cephfs`` are the object classes that make use of the 
  PGLSFilter class. To be able to use the filter functionality, one would need to include 
  ``objclass.h``.

Step 5: Next steps
------------------

The ``PGLSFilter`` class has not been made a part of the public interface in the initial version of the project. The ``init`` function of this class has dependecies on the encoding interface. We do not want to expose the internal implementation of ``encoding.h`` through the public interface. 

One way to go about it would be to use Google Protocol Buffers. However, it seems that the ``PGLSFilter`` class's initialization is done using multiple calls to the ``decode`` function, out of which the first call is made in ``osd/ReplicatedPG.cc``. Depending on the type of the filter(parent or plain), further decoding is done to determine other parameters. As a first step, we could start by replacing the ``encode/decode`` calls in the object classes using encode/decode API functions, which make use of Google Protocol Buffers to serialize and deserialize bufferlists. However, due to the complexity involved and time contraints, such a step has not been taken yet. 

Once filters become a part of the public interface, we should ideally just have ``objclass-public.h``. This would also imply replacing existing usage of ``objclass.h`` in the entire `Ceph` tree. The final step would be to build rpm/debian packages for the object classes.
