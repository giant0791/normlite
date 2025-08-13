# normlite/connection.py
# Copyright (C) 2025 Gianmarco Antonini
#
# This module is part of normlite and is released under the GNU Affero General Public License.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from typing import Optional
from normlite.cursor import CursorResult
from normlite.notiondbapi.dbapi2 import Cursor
from normlite.sql.base import Executable


class Connection:
    """Provide high level API to a connection to Notion databases.

    This class delegates the low level implementation of its methods to the DBAPI counterpart
    :class:`dbapi2.Connection`.

    .. versionadded:: 0.7.0
    
    """

    def execute(self, stmt: Executable, parameters: Optional[dict]) -> CursorResult:
        """Execute an SQL statement.

        This method executes both DML and DDL statements in an enclosing (implicit) transaction.
        When it is called for the first time, it sets up the enclosing transaction.
        All subsequent calls to this method add the statements to the enclosing transaction.
        Use either :meth:`commit()` to commit the changes permanently or :meth:`rollback()` to
        rollback.

        Note:
            **Non-mutating** statements like ``SELECT`` returns their result immediately after the
            :meth:`Connection.execute()` returns. All **mutating** statements like ``INSERT``, 
            ``UPDATE`` or ``DELETE`` return an **empty** result immediately.

        Important:
            The cursor result object associated with the last :meth:`Connection.execute()` contains
            a list of all result sets of the statements executed within the enclosing transaction.

        Args:
            stmt (SqlNode): The statement to execute.
            parameters (Optional[dict]): An optional dictionary containing the parameters to be
            bound to the SQL statement.   

        Returns:
            CursorResult: The result of the statement execution as cursor result.

        .. versionadded:: 0.7.0

        """
        # get the underlying cursor to execute the statement
        cursor: Cursor = self.connection.cursor()

        # bind the parameters to the statement
        stmt.bindparams(parameters)
        
        # compile the statement (operation() and parameters() return values)
        stmt.compile()

        # execute the compiled statement with its compiled parameters
        cursor.execute(stmt.operation(), stmt.parameters())

        # return the cursor result
        return CursorResult(cursor)

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass
