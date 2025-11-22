
import pytest
from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)

def test_list_books():
    response = client.get("/api/v1/books")
    assert response.status_code == 200
    data = response.json()
    assert "items" in data
    assert "page" in data
    assert "total" in data

def test_get_book():
    response = client.get("/api/v1/books/1")
    assert response.status_code in [200, 404]

def test_search_books():
    response = client.get("/api/v1/books?q=orwell")
    assert response.status_code == 200