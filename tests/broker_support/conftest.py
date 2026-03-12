"""
Test configuration — patches Settings so unit tests never need a real env file.
Runs before any test collection, so imports of EToroClient / settings are safe.
"""
import os
import pytest

# Set env vars before any broker_support module is imported during collection.
os.environ.setdefault('ETORO_API_KEY', 'test-api-key')
os.environ.setdefault('ETORO_USER_KEY', 'test-user-key')
os.environ.setdefault('ETORO_BASE_URL', 'https://public-api.etoro.com')
os.environ.setdefault('ETORO_USERNAME', 'testuser')