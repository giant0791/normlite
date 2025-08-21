normlite.future.asdl_compiler
=============================

.. py:module:: normlite.future.asdl_compiler

.. autoapi-nested-parse::

   Prototype implementation of a Zephyr ASDL to Python AST node classes parser.









Module Contents
---------------

.. py:class:: Module

   .. py:attribute:: name
      :type:  str


   .. py:attribute:: types
      :type:  list[TypeDef]


.. py:class:: TypeDef

   .. py:attribute:: name
      :type:  str


   .. py:attribute:: alts
      :type:  list[Constructor] | None


   .. py:attribute:: product
      :type:  Constructor | None


   .. py:attribute:: is_enum
      :type:  bool
      :value: False



.. py:class:: Constructor

   .. py:attribute:: name
      :type:  str


   .. py:attribute:: fields
      :type:  list[Field]


.. py:class:: Field

   .. py:attribute:: name
      :type:  str


   .. py:attribute:: type
      :type:  TypeRef


.. py:class:: TypeRef

   .. py:attribute:: union
      :type:  list[SingleType]


   .. py:attribute:: opt
      :type:  bool
      :value: False



   .. py:attribute:: seq
      :type:  bool
      :value: False



.. py:class:: SingleType

   .. py:attribute:: name
      :type:  str


.. py:data:: _TOKEN_RE

.. py:data:: PRIMITIVES

.. py:class:: Tok(kind: str, val: str)

   .. py:attribute:: kind


   .. py:attribute:: val


   .. py:method:: __repr__()


.. py:function:: lex_asdl(src: str) -> list[Tok]

.. py:class:: Parser(toks: list[Tok])

   .. py:attribute:: toks


   .. py:attribute:: i
      :value: 0



   .. py:method:: peek() -> Tok | None


   .. py:method:: eat(kind: str = None, val: str = None) -> Tok


   .. py:method:: parse() -> Module


   .. py:method:: parse_typedef() -> TypeDef


   .. py:method:: parse_alt() -> Constructor


   .. py:method:: parse_field() -> Field


   .. py:method:: parse_typeref() -> TypeRef


   .. py:method:: parse_single() -> SingleType


.. py:class:: BaseAST(**kwargs)

   Base class for all AST nodes.


   .. py:attribute:: _fields
      :type:  tuple[str, Ellipsis]
      :value: ()



   .. py:method:: iter_fields()


   .. py:method:: __repr__()


.. py:class:: SelectStmt(**kwargs)

   Bases: :py:obj:`BaseAST`


   Base class for all AST nodes.


   .. py:attribute:: _fields
      :value: ('columns', 'from_table', 'where', 'order_by', 'limit')



.. py:class:: Column(**kwargs)

   Bases: :py:obj:`BaseAST`


   Base class for all AST nodes.


   .. py:attribute:: _fields
      :value: ('name', 'alias')



.. py:class:: OrderItem(**kwargs)

   Bases: :py:obj:`BaseAST`


   Base class for all AST nodes.


   .. py:attribute:: _fields
      :value: ('expr', 'direction')



.. py:class:: BinaryOp(**kwargs)

   Bases: :py:obj:`BaseAST`


   Base class for all AST nodes.


   .. py:attribute:: _fields
      :value: ('left', 'op', 'right')



.. py:class:: Identifier(**kwargs)

   Bases: :py:obj:`BaseAST`


   Base class for all AST nodes.


   .. py:attribute:: _fields
      :value: ('value',)



.. py:class:: Constant(**kwargs)

   Bases: :py:obj:`BaseAST`


   Base class for all AST nodes.


   .. py:attribute:: _fields
      :value: ('value',)



.. py:data:: ASC
   :value: 'ASC'


.. py:data:: DESC
   :value: 'DESC'


.. py:data:: Eq
   :value: '='


.. py:data:: Gt
   :value: '>'


.. py:data:: Lt
   :value: '<'


.. py:data:: Ge
   :value: '>='


.. py:data:: Le
   :value: '<='


.. py:data:: Ne
   :value: '!='


.. py:data:: And
   :value: 'AND'


.. py:data:: Or
   :value: 'OR'


