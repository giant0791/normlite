cursor
======

.. py:module:: cursor




Module Contents
---------------

.. py:class:: _FrozenAttributeMixin(*, frozen_attributes: set[str] = None)

   A mixin that prevents setting or deleting attributes listed in self._frozen_attributes.

   Uses object-level access to avoid recursion during attribute handling. It is used by :class:`Row` to
   implement read-only attribute for the row columns to access the corresponding values


   .. py:attribute:: __slots__
      :value: ('_frozen_attributes',)



   .. py:method:: __setattr__(name: str, value: Any) -> NoReturn


   .. py:method:: __delattr__(name: str) -> NoReturn


   .. py:method:: _get_frozen_attributes() -> set[str]


   .. py:method:: __getattr__(name: str) -> NoReturn


.. py:class:: Row(metadata: normlite.cursor.CursorResultMetaData, row_data: tuple)

   Bases: :py:obj:`_FrozenAttributeMixin`


   Provide pythonic high level interface to a single SQL database row.


   .. py:method:: __getitem__(key_or_index: Union[str, int]) -> Any


   .. py:method:: mapping() -> dict


   .. py:method:: __repr__()


