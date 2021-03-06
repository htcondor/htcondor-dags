API Reference
=============

.. py:currentmodule:: htcondor_dags

.. attention::
    This is not documentation for DAGMan itself! If you run into DAGMan jargon
    that isn't explained here, see `The DAGMan Manual <https://htcondor.readthedocs.io/en/latest/users-manual/dagman-applications.html>`_.

Creating DAGs
-------------

.. autoclass:: DAG
   :members:

.. autoclass:: WalkOrder


Nodes and Node-likes
++++++++++++++++++++

.. autoclass:: BaseNode
   :members:

.. autoclass:: NodeLayer
   :members:

.. autoclass:: SubDAG
   :members:

.. autoclass:: FinalNode

.. autoclass:: Nodes
   :members:


Edges
+++++

.. autoclass:: OneToOne
.. autoclass:: ManyToMany
.. autoclass:: Grouper
.. autoclass:: Slicer


Node Configuration
++++++++++++++++++

.. autoclass:: Script

.. autoclass:: DAGAbortCondition


Writing a DAG to Disk
+++++++++++++++++++++

.. autofunction:: write_dag

.. autoclass:: NodeNameFormatter

.. autoclass:: SimpleFormatter


DAG Configuration
-----------------

.. autoclass:: DotConfig

.. autoclass:: NodeStatusFile


Rescue DAGs
-----------

.. autofunction:: rescue

.. autofunction:: find_rescue_file
