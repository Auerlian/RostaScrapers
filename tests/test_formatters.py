"""Unit tests for CSV field formatters."""

import pytest
from datetime import datetime

from src.export.formatters import (
    format_list,
    format_null,
    format_boolean,
    format_datetime
)


class TestFormatList:
    """Tests for format_list function."""
    
    def test_format_list_with_items(self):
        """Test formatting list with multiple items."""
        items = ["tag1", "tag2", "tag3"]
        result = format_list(items)
        assert result == "tag1;tag2;tag3"
    
    def test_format_list_single_item(self):
        """Test formatting list with single item."""
        items = ["single"]
        result = format_list(items)
        assert result == "single"
    
    def test_format_list_empty(self):
        """Test formatting empty list."""
        result = format_list([])
        assert result == ""
    
    def test_format_list_none(self):
        """Test formatting None list."""
        result = format_list(None)
        assert result == ""
    
    def test_format_list_with_special_chars(self):
        """Test formatting list with special characters."""
        # CSV module will handle escaping, we just join with semicolons
        items = ["tag,with,commas", "tag;with;semicolons", 'tag"with"quotes']
        result = format_list(items)
        assert result == 'tag,with,commas;tag;with;semicolons;tag"with"quotes'
    
    def test_format_list_with_spaces(self):
        """Test formatting list with items containing spaces."""
        items = ["multi word tag", "another tag", "third"]
        result = format_list(items)
        assert result == "multi word tag;another tag;third"


class TestFormatNull:
    """Tests for format_null function."""
    
    def test_format_null_with_none(self):
        """Test formatting None value."""
        result = format_null(None)
        assert result == ""
    
    def test_format_null_with_string(self):
        """Test formatting string value."""
        result = format_null("hello")
        assert result == "hello"
    
    def test_format_null_with_number(self):
        """Test formatting numeric value."""
        result = format_null(42)
        assert result == "42"
    
    def test_format_null_with_float(self):
        """Test formatting float value."""
        result = format_null(3.14)
        assert result == "3.14"
    
    def test_format_null_with_zero(self):
        """Test formatting zero (should not be treated as null)."""
        result = format_null(0)
        assert result == "0"
    
    def test_format_null_with_empty_string(self):
        """Test formatting empty string (should not be treated as null)."""
        result = format_null("")
        assert result == ""


class TestFormatBoolean:
    """Tests for format_boolean function."""
    
    def test_format_boolean_true(self):
        """Test formatting True value."""
        result = format_boolean(True)
        assert result == "true"
    
    def test_format_boolean_false(self):
        """Test formatting False value."""
        result = format_boolean(False)
        assert result == "false"
    
    def test_format_boolean_none(self):
        """Test formatting None value."""
        result = format_boolean(None)
        assert result == ""


class TestFormatDatetime:
    """Tests for format_datetime function."""
    
    def test_format_datetime_with_datetime(self):
        """Test formatting datetime object."""
        dt = datetime(2025, 1, 15, 10, 30, 0)
        result = format_datetime(dt)
        assert result == "2025-01-15T10:30:00Z"
    
    def test_format_datetime_none(self):
        """Test formatting None datetime."""
        result = format_datetime(None)
        assert result == ""
    
    def test_format_datetime_midnight(self):
        """Test formatting datetime at midnight."""
        dt = datetime(2025, 1, 1, 0, 0, 0)
        result = format_datetime(dt)
        assert result == "2025-01-01T00:00:00Z"
    
    def test_format_datetime_end_of_day(self):
        """Test formatting datetime at end of day."""
        dt = datetime(2025, 12, 31, 23, 59, 59)
        result = format_datetime(dt)
        assert result == "2025-12-31T23:59:59Z"
    
    def test_format_datetime_with_microseconds(self):
        """Test formatting datetime with microseconds (should be truncated)."""
        dt = datetime(2025, 1, 15, 10, 30, 0, 123456)
        result = format_datetime(dt)
        # ISO format without microseconds
        assert result == "2025-01-15T10:30:00Z"


class TestCSVEscaping:
    """Tests to verify CSV escaping behavior.
    
    Note: The Python csv module handles escaping automatically when using
    csv.DictWriter. These tests document expected behavior for strings
    that will be passed to the csv module.
    """
    
    def test_string_with_comma(self):
        """Test that strings with commas are preserved."""
        # The csv module will quote this when writing
        value = "Hello, World"
        result = format_null(value)
        assert result == "Hello, World"
    
    def test_string_with_quotes(self):
        """Test that strings with quotes are preserved."""
        # The csv module will escape quotes when writing
        value = 'He said "Hello"'
        result = format_null(value)
        assert result == 'He said "Hello"'
    
    def test_string_with_newline(self):
        """Test that strings with newlines are preserved."""
        # The csv module will quote this when writing
        value = "Line 1\nLine 2"
        result = format_null(value)
        assert result == "Line 1\nLine 2"
    
    def test_string_with_carriage_return(self):
        """Test that strings with carriage returns are preserved."""
        value = "Line 1\r\nLine 2"
        result = format_null(value)
        assert result == "Line 1\r\nLine 2"
    
    def test_list_with_commas_in_items(self):
        """Test list items containing commas."""
        items = ["item, with comma", "normal item"]
        result = format_list(items)
        # Semicolon separator avoids comma conflicts
        assert result == "item, with comma;normal item"
    
    def test_list_with_quotes_in_items(self):
        """Test list items containing quotes."""
        items = ['item "with" quotes', "normal item"]
        result = format_list(items)
        assert result == 'item "with" quotes;normal item'
