normlite.notiondbapi._parser
============================

.. py:module:: normlite.notiondbapi._parser

.. autoapi-nested-parse::

   Provide the parser for Notion API objects.

   This module implements a simple Notion API objects parser for constructing an AST, which
   is used by the visitor for cross-compilation of Notion JSON objects into tuples of elements.

   .. important::

      This module is **private** to the package :mod:`notiondbapi` and it does **not** expose
      its features outside.





Module Contents
---------------

.. py:function:: parse_text_content(values: list) -> str

.. py:function:: parse_number(number: Union[dict, int, float]) -> Tuple[str, Union[int, float, None]]

   Parse a number object.

   This method parses the following number objects:

       >>> # empty number
       >>> number = None
       >>> ptype, value = parse_number(number)
       >>> ptype, value
       ('number', None)

       >>> # number with format spec
       >>> number = {"format": "dollar"}
       >>> ptype, value = parse_number(number)
       >>> ptype, value
       ('number.dollar', None)

       >>> # number with numeric value (int or float)
       >>> number = 2
       >>> ptype, value = parse_number(number)
       >>> ptype, value
       ('number', 2)

   :param number: Either a dictionary containing a number spec or a numeric value.
   :type number: Union[dict, int, float]

   :returns: The pair (type, value) as tuple.
   :rtype: Optional[Tuple[str, Union[int, float, None]]]


.. py:function:: parse_property(name: str, payload: dict) -> normlite.notiondbapi._model.NotionProperty

   Parse a JSON property object

   This method parses a JSON property object and creates the corresponding Python node object.

   :param name: The property name.
   :type name: str
   :param payload: A dictionary containing the JSON property object.
   :type payload: dict

   :raises TypeError: If an unexpected or unsupported property type is in the :obj:`payload`.

   :returns: The Python node object corresponding to the Notion page or database property.
   :rtype: NotionProperty


.. py:function:: parse_page(payload: dict) -> normlite.notiondbapi._model.NotionPage

.. py:function:: parse_database(payload: dict) -> normlite.notiondbapi._model.NotionDatabase

