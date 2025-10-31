import re

def drop_comments(ddl: str) -> str:
    """
    Drop commented fields within a sql.
    """
    no_comments = re.sub(r"/\*.*?\*/", "", ddl, flags=re.DOTALL)
    no_comments = re.sub(r"--.*", "", no_comments)
    return no_comments