import re

def drop_comments(ddl: str) -> str:
    """
    Drop commented fields within a sql.
    """
    no_comments = re.sub(r"/\*.*?\*/", "", ddl, flags=re.DOTALL)
    no_comments = re.sub(r"--.*", "", no_comments)
    return no_comments

def get_from(ddl: str) -> str:
    """
    Extract the FROM object (source) from an SQL query.
    """
    from_object = re.search(r'FROM\s+(\{[^}]+\}\.\{[^}]+\}\.[A-Za-z0-9_]+)', string= ddl).group(1)
    return from_object