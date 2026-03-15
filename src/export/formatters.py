"""Field formatting functions for CSV export.

This module provides formatting functions to convert Python data types
into CSV-friendly string representations. The Python csv module handles
CSV escaping automatically (quotes, commas, newlines), so these functions
focus on type conversion and standardization.
"""

from datetime import datetime
from typing import Any


def format_list(items: list[str] | None) -> str:
    """Format list as semicolon-separated string.
    
    Lists are joined with semicolons to avoid conflicts with CSV commas.
    The csv module will automatically escape the result if it contains
    special characters.
    
    Args:
        items: List of strings or None
    
    Returns:
        Semicolon-separated string or empty string for None/empty list
    
    Examples:
        >>> format_list(["tag1", "tag2", "tag3"])
        'tag1;tag2;tag3'
        >>> format_list([])
        ''
        >>> format_list(None)
        ''
    """
    if not items:
        return ""
    return ";".join(items)


def format_null(value: Any) -> str:
    """Format null/None values as empty string.
    
    Converts None to empty string for CSV export. Non-None values
    are converted to strings using str().
    
    Args:
        value: Any value
    
    Returns:
        String representation or empty string for None
    
    Examples:
        >>> format_null(None)
        ''
        >>> format_null("hello")
        'hello'
        >>> format_null(42)
        '42'
    """
    if value is None:
        return ""
    return str(value)


def format_boolean(value: bool | None) -> str:
    """Format boolean as lowercase string.
    
    Converts boolean values to lowercase "true" or "false" strings
    for consistency with JSON conventions. None values become empty strings.
    
    Args:
        value: Boolean value or None
    
    Returns:
        "true", "false", or empty string for None
    
    Examples:
        >>> format_boolean(True)
        'true'
        >>> format_boolean(False)
        'false'
        >>> format_boolean(None)
        ''
    """
    if value is None:
        return ""
    return "true" if value else "false"


def format_datetime(dt: datetime | None) -> str:
    """Format datetime as ISO 8601 UTC string.
    
    Converts datetime objects to ISO 8601 format (YYYY-MM-DDTHH:MM:SSZ).
    Assumes the datetime is in UTC. None values become empty strings.
    
    Args:
        dt: Datetime object or None
    
    Returns:
        ISO 8601 formatted string or empty string for None
    
    Examples:
        >>> from datetime import datetime
        >>> dt = datetime(2025, 1, 15, 10, 30, 0)
        >>> format_datetime(dt)
        '2025-01-15T10:30:00Z'
        >>> format_datetime(None)
        ''
    """
    if dt is None:
        return ""
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
