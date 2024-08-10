def strip_table_prefix(table_name: str) -> str:
    if table_name.startswith("iasworld"):
        return table_name.split(".", 1)[1]
    return table_name
