class Cursor:
    """Implement the `Cursor` class according to the DBAPI 2.0 specification."""
    def __init__(self):
        self._result_set = {}  # set externally or by another method

    def fetchall(self):
        results = self._result_set.get("results", [])
        rows = []

        for page in results:
            row = []
            properties = page.get("properties", {})
            for column_name, column_data in properties.items():
                db_type = column_data.get("type")

                # Extract value based on type
                if db_type == "number":
                    value = column_data.get("number", "")
                elif db_type == "rich-text":
                    items = column_data.get("richt-text", [])
                    value = items[0]["text"]["content"] if items else ""
                elif db_type == "title":
                    items = column_data.get("title", [])
                    value = items[0]["text"]["content"] if items else ""
                else:
                    value = ""

                row.append((column_name, db_type, str(value)))

            rows.append(row)

        return rows
