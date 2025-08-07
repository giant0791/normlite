normlite.proxy.routes
=====================

.. py:module:: normlite.proxy.routes

.. autoapi-nested-parse::

   Transaction Proxy Server Routes.

   This module provides the Flask routes that constitute the REST API for the transaction proxy server.
   All endpoints return JSON objects of the following form:

   .. code-block:: json

       {
           "transaction_id", "<tx_id>",    // <tx_id> = uuid4 string, returned by POST /transactions only
           "state": "<tx_state>",          // <tx_state> = transaction state, returned by
                                           // POST /transactions/<id>/insert
                                           // POST /transactions/<id>/commit
                                           // POST /transactions/<id>/rollback
           "data": "<result_sets>",        // <result_sets> = a list containing all results returned by each operation committed/rolled back
           "error": "<error>"              // <error> = error message string, returned in case of error only
       }

   .. list-table:: Transaction Proxy Server REST API
      :header-rows: 1
      :widths: 15, 25, 60
      :class: longtable

      * - Method
        - Endpoint
        - Description
      * - ``POST``
        - ``/transactions``
        - Begin a new transaction (see :func:`normlite.proxy.routes.transactions.begin_transaction()` for more details).
      * - ``POST``
        - ``/transactions/<id>/insert``
        - Add an inser operation to an existing transaction (see :func:`normlite.proxy.routes.insert.insert()` for more details).
      * - ``POST``
        - ``/transactions/<id>/commit``
        - Commit an existing transaction (see :func:`normlite.proxy.routes.transactions.commit_transaction()` for more details).
      * - ``POST``
        - ``/transactions/<id>/rollback``
        - Roll back an existing transaction (see :func:`normlite.proxy.routes.transactions.rollback_transaction()` for more details).

   .. versionadded:: 0.6.0



Submodules
----------

.. toctree::
   :maxdepth: 1

   /autoapi/normlite/proxy/routes/insert/index
   /autoapi/normlite/proxy/routes/transactions/index




Package Contents
----------------

.. py:function:: _make_response_obj(obj: dict, tx_id: Optional[str] = None) -> flask.Response

   Helper to generate standard response objects.


